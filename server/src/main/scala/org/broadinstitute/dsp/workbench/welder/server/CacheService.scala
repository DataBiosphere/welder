package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path

import cats.effect.{ContextShift, IO}
import cats.implicits._
import fs2.Stream
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext

class CacheService(config: CachedServiceConfig, storageLinksCache: StorageLinksCache, metadataCache: MetadataCache, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
) extends Http4sDsl[IO] {

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case POST -> Root / "flush" =>
      flush >> NoContent()
  }

  val flush: IO[Unit] = {
    val flushStorageLinks = flushCache(config.storageLinksPath, blockingEc, storageLinksCache)
    val flushMetadataCache = flushCache(config.metadataCachePath, blockingEc, metadataCache)
    Stream(flushMetadataCache, flushStorageLinks).parJoin(2).compile.drain
  }
}

object CacheService {
  def apply(config: CachedServiceConfig, storageLinksCache: StorageLinksCache, metadataCache: MetadataCache, blockingEc: ExecutionContext)(
      implicit cs: ContextShift[IO]
  ): CacheService = new CacheService(config, storageLinksCache, metadataCache, blockingEc)
}

final case class CachedServiceConfig(storageLinksPath: Path, metadataCachePath: Path)
