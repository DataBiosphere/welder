package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.TimeUnit
import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.Ask
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath

import java.io.File
import scala.concurrent.duration.FiniteDuration

class BackgroundTask(
    config: BackgroundTaskConfig,
    metadataCache: MetadataCache,
    storageLinksCache: StorageLinksCache,
    googleStorageAlg: GoogleStorageAlg,
    metadataCacheAlg: MetadataCacheAlg,
    googleStorageService: GoogleStorageService[IO],
    blocker: Blocker
)(implicit cs: ContextShift[IO], logger: Logger[IO], timer: Timer[IO]) {
  val cleanUpLock: Stream[IO, Unit] = {
    val task = (for {
      now <- timer.clock.monotonic(TimeUnit.MILLISECONDS)
      updatedMap <- metadataCache.modify { mp =>
        val newMap = mp.map { kv =>
          kv._2.remoteState match {
            case RemoteState.NotFound => kv
            case RemoteState.Found(lock, crc32c) =>
              val newLock = lock.filter(l => l.lockExpiresAt.toEpochMilli < now)
              (kv._1 -> kv._2.copy(remoteState = RemoteState.Found(newLock, crc32c))) //This can be a bit cleaner with monocle
          }
        }
        (newMap, newMap)
      }
      _ <- logger.info(s"updated metadata cache size ${updatedMap.size}")
      _ <- logger.debug(s"updated metadata cache ${updatedMap}")
    } yield ()).handleErrorWith(t => logger.error(t)("fail to update metadata cache"))

    (Stream.sleep[IO](config.cleanUpLockInterval) ++ Stream.eval(task)).repeat
  }

  def flushBothCache(
      storageLinksJsonBlobName: GcsBlobName,
      gcsMetadataJsonBlobName: GcsBlobName,
      blocker: Blocker
  ): Stream[IO, Unit] = {
    val flushStorageLinks = flushCache(googleStorageAlg, config.stagingBucket, storageLinksJsonBlobName, storageLinksCache).handleErrorWith { t =>
      Stream.eval(logger.info(t)("failed to flush storagelinks cache to GCS"))
    }
    val flushMetadataCache = flushCache(googleStorageAlg, config.stagingBucket, gcsMetadataJsonBlobName, metadataCache).handleErrorWith { t =>
      Stream.eval(logger.info(t)("failed to flush metadata cache to GCS"))
    }
    (Stream.sleep[IO](config.flushCacheInterval) ++ flushStorageLinks ++ flushMetadataCache).repeat
  }

  val syncCloudStorageDirectory: Stream[IO, Unit] = {
    val res = for {
      storageLinks <- storageLinksCache.get
      traceId <- IO(TraceId(UUID.randomUUID().toString))
      _ <- storageLinks.values.toList.traverse { storageLink =>
        logger.info(s"syncing file from ${storageLink.cloudStorageDirectory}") >>
          (googleStorageAlg
            .localizeCloudDirectory(
              storageLink.localBaseDirectory.path,
              storageLink.cloudStorageDirectory,
              config.workingDirectory,
              storageLink.pattern,
              traceId
            )
            .through(metadataCacheAlg.updateCachePipe))
            .compile
            .drain
      }
    } yield ()

    (Stream.sleep[IO](config.syncCloudStorageDirectoryInterval) ++ Stream.eval(res)).repeat
  }

  val delocalizeBackgroundProcess: Stream[IO, Unit] = {
    if (config.isRstudioRuntime) {
      val res = for {
        storageLinks <- storageLinksCache.get
        implicit0(tid: Ask[IO, TraceId]) <- IO(TraceId(UUID.randomUUID().toString)).map(tid => Ask.const[IO, TraceId](tid))
        _ <- storageLinks.values.toList.traverse { storageLink =>
          if (storageLink.pattern.toString.contains(".Rmd")) {
            findFilesWithPattern(config.workingDirectory.resolve(storageLink.localBaseDirectory.path.asPath), storageLink.pattern).traverse_ { file =>
              val gsPath = getGsPath(storageLink, new File(file.getName))
              val localAbsolutePath = config.workingDirectory.resolve(storageLink.localBaseDirectory.path.asPath).resolve(file.getName)
              for {
                sd <- shouldDelocalize(gsPath, localAbsolutePath)
                _ <- if (sd) {
                  googleStorageAlg.fileToGcs(
                    RelativePath(java.nio.file.Paths.get(file.getName)),
                    gsPath
                  )
                } else {
                  IO.unit
                }
              } yield ()

            }
          } else IO.unit
        }
      } yield ()
      (Stream.sleep[IO](config.delocalizeDirectoryInterval) ++ Stream.eval(res)).repeat
    } else {
      Stream.eval(logger.info("Not running rmd sync process because this is not an Rstudio runtime"))
    }
  }

  def shouldDelocalize(gsPath: GsPath, localAbsolutePath: Path): IO[Boolean] =
    for {
      meta <- googleStorageService.getObjectMetadata(gsPath.bucketName, gsPath.blobName, None).compile.last
      _ <- logger.info(s"!!!meta: ${meta.getOrElse("nada").toString}")
      localCrc32c <- Crc32c.calculateCrc32ForFile(localAbsolutePath, blocker)
      _ <- logger.info(s"!!!c: ${localCrc32c}")
    } yield meta match {
      case Some(GetMetadataResponse.Metadata(crc32, _, _)) =>
        if (localCrc32c == crc32)
          false
        else
          true
      case _ => true
    }

  def getGsPath(storageLink: StorageLink, file: File): GsPath = {
    val fullBlobPath = getFullBlobName(
      storageLink.localBaseDirectory.path,
      storageLink.localBaseDirectory.path.asPath.resolve(file.toString),
      storageLink.cloudStorageDirectory.blobPath
    )
    GsPath(storageLink.cloudStorageDirectory.bucketName, fullBlobPath)
  }
}

final case class BackgroundTaskConfig(
    workingDirectory: Path,
    stagingBucket: GcsBucketName,
    cleanUpLockInterval: FiniteDuration,
    flushCacheInterval: FiniteDuration,
    syncCloudStorageDirectoryInterval: FiniteDuration,
    delocalizeDirectoryInterval: FiniteDuration,
    isRstudioRuntime: Boolean
)
