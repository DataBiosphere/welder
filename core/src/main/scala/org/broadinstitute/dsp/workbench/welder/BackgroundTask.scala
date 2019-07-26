package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.TimeUnit

import cats.effect.{ContextShift, IO, Timer}
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import JsonCodec._
import cats.implicits._
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.ExecutionContext
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
          val newLock = kv._2.remoteState.lock.filter(l => l.lockExpiresAt.toEpochMilli < now)
          (kv._1 -> kv._2.copy(remoteState = RemoteState(newLock, kv._2.remoteState.crc32c))) //This can be a bit cleaner with monocle
        }
        (newMap, newMap)
      }
      _ <- logger.info(s"updated metadata cache size ${updatedMap.size}")
      _ <- logger.debug(s"updated metadata cache ${updatedMap}")
    } yield ()).handleErrorWith(t => logger.error(t)("fail to update metadata cache"))

    (Stream.sleep[IO](config.cleanUpLockInterval) ++ Stream.eval(task)).repeat
  }

  def flushBothCache(
      storageLinksPath: Path,
      metadataCachePath: Path,
      blockingEc: ExecutionContext
  ): Stream[IO, Unit] = {
    val flushStorageLinks = flushCache(storageLinksPath, blockingEc, storageLinksCache).handleErrorWith { t =>
      Stream.eval(logger.info(t)("failed to flush storagelinks cache to disk"))
    }
    val flushMetadataCache = flushCache(metadataCachePath, blockingEc, metadataCache).handleErrorWith { t =>
      Stream.eval(logger.info(t)("failed to flush metadata cache to disk"))
    }
    (Stream.sleep[IO](config.flushCacheInterval) ++ flushStorageLinks ++ flushMetadataCache).repeat
  }

  val syncCloudStorageDirectory: Stream[IO, Unit] = {
    val res = for {
      storageLinks <- storageLinksCache.get
      traceId <- IO(TraceId(UUID.randomUUID()))
      _ <- storageLinks.values.toList.traverse { storageLink =>
        logger.info(s"syncing file from ${storageLink.cloudStorageDirectory}") >>
        (googleStorageAlg
          .localizeCloudDirectory(storageLink.localBaseDirectory.path, storageLink.cloudStorageDirectory, config.workingDirectory, storageLink.pattern, traceId)
          .through(metadataCacheAlg.updateCachePipe))
          .compile
          .drain
      }
    } yield ()

    (Stream.sleep[IO](config.syncCloudStorageDirectoryInterval) ++ Stream.eval(res)).repeat
  }
}

final case class BackgroundTaskConfig(
    workingDirectory: Path,
    cleanUpLockInterval: FiniteDuration,
    flushCacheInterval: FiniteDuration,
    syncCloudStorageDirectoryInterval: FiniteDuration
)
