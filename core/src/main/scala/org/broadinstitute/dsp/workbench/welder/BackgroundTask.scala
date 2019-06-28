package org.broadinstitute.dsp.workbench.welder

import java.util.concurrent.TimeUnit

import cats.effect.{IO, Timer}
import fs2.Stream
import io.chrisdavenport.log4cats.Logger

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
      _ <- logger.info(s"updated metadata cache ${updatedMap.take(100)}")
    } yield ()).handleErrorWith(t => logger.error(t)("fail to update metadata cache"))

    (Stream.sleep[IO](interval) ++ Stream.eval(task)).repeat
  }
}
