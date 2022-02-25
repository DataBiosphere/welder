package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path
import java.util.UUID
import cats.effect.IO
import cats.implicits._
import cats.mtl.Ask
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
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
    metadataCacheAlg: MetadataCacheAlg
)(implicit logger: Logger[IO]) {
  val cleanUpLock: Stream[IO, Unit] = {
    val task = (for {
      now <- IO.realTimeInstant
      updatedMap <- metadataCache.modify { mp =>
        val newMap = mp.map { kv =>
          kv._2.remoteState match {
            case RemoteState.NotFound => kv
            case RemoteState.Found(lock, crc32c) =>
              val newLock = lock.filter(l => l.lockExpiresAt.toEpochMilli < now.toEpochMilli)
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
      gcsMetadataJsonBlobName: GcsBlobName
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
      val res = (for {
        storageLinks <- storageLinksCache.get
        implicit0(tid: Ask[IO, TraceId]) <- IO(TraceId(UUID.randomUUID().toString)).map(tid => Ask.const[IO, TraceId](tid))
        _ <- storageLinks.values.toList.traverse { storageLink =>
          if (storageLink.pattern.toString.contains(".Rmd")) {
            findFilesWithPattern(config.workingDirectory.resolve(storageLink.localBaseDirectory.path.asPath), storageLink.pattern).traverse_ { file =>
              val gsPath = getGsPath(storageLink, new File(file.getName))
              safeDelocalize(
                gsPath,
                RelativePath(java.nio.file.Paths.get(file.getName))
              )
            }
          } else IO.unit
        }
      } yield ()).handleErrorWith(r => logger.info(r)(s"Unexpected error encountered ${r}"))
      (Stream.sleep[IO](config.delocalizeDirectoryInterval) ++ Stream.eval(res)).repeat
    } else {
      Stream.eval(logger.info("Not running rmd sync process because this is not an Rstudio runtime"))
    }
  }

  def safeDelocalize(gsPath: GsPath, localObjectPath: RelativePath)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    for {
      traceId <- ev.ask[TraceId]
      localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
      previousMeta <- metadataCacheAlg.getCache(localObjectPath)
      calculatedCrc32c <- Crc32c.calculateCrc32ForFile(localAbsolutePath)

      _ <- previousMeta match {
        case Some(meta) =>
          meta.remoteState match {
            case RemoteState.NotFound =>
              delocalizeAndUpdateCache(localObjectPath, gsPath, 0L, traceId)
            case RemoteState.Found(_, crc32c) =>
              if (calculatedCrc32c == crc32c)
                IO.unit
              else
                delocalizeAndUpdateCache(localObjectPath, gsPath, meta.localFileGeneration.getOrElse(0L), traceId)
          }
        case None =>
          delocalizeAndUpdateCache(localObjectPath, gsPath, 0L, traceId)
      }
    } yield ()

  private def delocalizeAndUpdateCache(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, traceId: TraceId): IO[Unit] = {
    val hashedOwnerEmail = IO.fromEither(hashString(config.ownerEmail.value))
    val lastModifiedByMetadataToPush: Map[String, String] = Map("lastModifiedBy" -> hashedOwnerEmail.toString())
    for {
      delocalizeResp <- googleStorageAlg
        .delocalize(localObjectPath, gsPath, generation, lastModifiedByMetadataToPush, traceId)
        .recoverWith {
          case e: GenerationMismatch =>
            if (generation == 0L)
              IO.raiseError(e)
            else {
              // In the case when the file is already been deleted from GCS, we try to delocalize the file with generation being 0L
              // This assumes the business logic we want is always to recreate files that have been deleted from GCS by other users.
              // If the file is indeed out of sync with remote, both delocalize attempts will fail due to generation mismatch
              googleStorageAlg.delocalize(
                localObjectPath,
                gsPath,
                0L,
                lastModifiedByMetadataToPush,
                traceId
              )
            }
        }
        .recoverWith {
          case e: GenerationMismatch =>
            googleStorageAlg.updateMetadata(gsPath, traceId, Map(hashedOwnerEmail.toString() -> "outdated")) >> IO.raiseError[DelocalizeResponse](e)
        }
      _ <- metadataCacheAlg.updateLocalFileStateCache(localObjectPath, RemoteState.Found(None, delocalizeResp.crc32c), delocalizeResp.generation)
    } yield ()
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
    isRstudioRuntime: Boolean,
    ownerEmail: WorkbenchEmail
)
