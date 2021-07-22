package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.TimeUnit

import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.implicits._
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath

import scala.concurrent.duration.FiniteDuration

class BackgroundTask(
    config: BackgroundTaskConfig,
    metadataCache: MetadataCache,
    storageLinksCache: StorageLinksCache,
    googleStorageAlg: GoogleStorageAlg,
    metadataCacheAlg: MetadataCacheAlg
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
    val res = for {
      storageLinks <- storageLinksCache.get
      traceId <- IO(TraceId(UUID.randomUUID().toString))
      _ <- storageLinks.values.toList.traverse { storageLink =>
        logger.info(s"syncing file from ${storageLink.localBaseDirectory}")
        findFilesWithSuffix(config.workingDirectory.resolve(storageLink.localBaseDirectory.path.asPath), ".Rmd").traverse_ { file =>
          val fullBlobPath = getFullBlobName(
            storageLink.localBaseDirectory.path,
            storageLink.localBaseDirectory.path.asPath.resolve(file.toString),
            storageLink.cloudStorageDirectory.blobPath
          )
          val gsPath = GsPath(storageLink.cloudStorageDirectory.bucketName, fullBlobPath)
          googleStorageAlg.delocalize(storageLink.localBaseDirectory.path, gsPath, 0L, Map.empty, traceId)
        }
      }
    } yield ()

    (Stream.sleep[IO](config.syncCloudStorageDirectoryInterval) ++ Stream.eval(res)).repeat
  }
}

final case class BackgroundTaskConfig(
    workingDirectory: Path,
    stagingBucket: GcsBucketName,
    cleanUpLockInterval: FiniteDuration,
    flushCacheInterval: FiniteDuration,
    syncCloudStorageDirectoryInterval: FiniteDuration,
    delocalizeDirectoryInterval: FiniteDuration
)
