package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path
import java.util.concurrent.TimeUnit
import cats.effect.{ContextShift, IO, Timer}
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import JsonCodec._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object BackgroundTask {
  def cleanUpLock(metadataCache: MetadataCache, interval: FiniteDuration)(
      implicit logger: Logger[IO],
      timer: Timer[IO]
  ): Stream[IO, Unit] = {
    val task = (for {
      now <- timer.clock.monotonic(TimeUnit.MILLISECONDS)
      updatedMap <- metadataCache.modify { mp =>
        val newMap = mp.map { kv =>
          val newLock = kv._2.lock.filter(l => l.lockExpiresAt.toEpochMilli < now)
          (kv._1 -> kv._2.copy(lock = newLock))
        }
        (newMap, newMap)
      }
      _ <- logger.info(s"updated metadata cache size ${updatedMap.size}")
      _ <- logger.debug(s"updated metadata cache ${updatedMap}")
    } yield ()).handleErrorWith(t => logger.error(t)("fail to update metadata cache"))

    (Stream.sleep[IO](interval) ++ Stream.eval(task)).repeat
  }

  def flushBothCache(interval: FiniteDuration,
                      storageLinksPath: Path,
                 metadataCachePath: Path,
                 storageLinksCache: StorageLinksCache,
                 metadataCache: MetadataCache,
                 blockingEc: ExecutionContext)(implicit cs: ContextShift[IO], logger: Logger[IO], timer: Timer[IO]): Stream[IO, Unit] = {
    val flushStorageLinks = flushCache(storageLinksPath, blockingEc, storageLinksCache).handleErrorWith { t =>
      Stream.eval(logger.info(t)("failed to flush storagelinks cache to disk"))
    }
    val flushMetadataCache = flushCache(metadataCachePath, blockingEc, metadataCache).handleErrorWith { t =>
      Stream.eval(logger.info(t)("failed to flush metadata cache to disk"))
    }
    (Stream.sleep[IO](interval) ++ flushStorageLinks ++ flushMetadataCache).repeat
  }
}