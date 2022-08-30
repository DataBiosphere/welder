package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.{IO, Ref}
import cats.implicits._
import cats.mtl.Ask
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.StructuredLogger

import java.nio.file.Path
import java.util.UUID

// This endpoint is called by leonardo before it tells dataproc to shut down user's vm.
class ShutdownService(
    config: PreshutdownServiceConfig,
    shutDownSignal: SignallingRef[IO, Boolean],
    storageLinksCache: StorageLinksCache,
    metadataCache: MetadataCache,
    storageAlgRef: Ref[IO, CloudStorageAlg]
)(implicit logger: StructuredLogger[IO])
    extends Http4sDsl[IO] {

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] { case POST -> Root / "flush" =>
    flush >> NoContent()
  }

  val flush: IO[Unit] = {

    implicit val traceIdImplicit = Ask.const[IO, TraceId](TraceId(UUID.randomUUID().toString))
    for {
      storageAlg <- storageAlgRef.get

      storageLinksSourceUri <- getSourceUriForProvider(storageAlg.cloudProvider, config.stagingBucketName, config.storageLinksJsonBlobName)
      metadataSourceUri <- getSourceUriForProvider(storageAlg.cloudProvider, config.stagingBucketName, config.gcsMetadataJsonBlobName)
      flushStorageLinks = flushCache(storageAlg, storageLinksSourceUri, storageLinksCache)
      flushMetadataCache = flushCache(storageAlg, metadataSourceUri, metadataCache)

      // Copy all welder log files and jupyter log file to staging bucket
      flushLogFiles = for {
        _ <- findFilesWithSuffix(config.workingDirectory, ".log").traverse_ { file =>
          val blobName = CloudStorageBlob(s"cluster-log-files/${file.getName}")
          getSourceUriForProvider(storageAlg.cloudProvider, config.stagingBucketName, blobName).flatMap { sourceUri =>
            storageAlg.fileToGcsAbsolutePath(file.toPath, sourceUri)
          }
        }
      } yield ()

      streams = Stream.eval(flushStorageLinks) ++ Stream.eval(flushMetadataCache) ++ Stream.eval(flushLogFiles)
      _ <- StructuredLogger[IO].info("Shutting down welder") >> Stream(streams)
        .parJoin(3)
        .compile
        .drain
      _ <- IO(shutDownSignal.update(_ => true)).void //shut down http server
    } yield ()

  }
}

object ShutdownService {
  def apply(
      config: PreshutdownServiceConfig,
      shutDownSignal: SignallingRef[IO, Boolean],
      storageLinksCache: StorageLinksCache,
      metadataCache: MetadataCache,
      storageAlgRef: Ref[IO, CloudStorageAlg]
  )(implicit
      logger: StructuredLogger[IO]
  ): ShutdownService = new ShutdownService(config, shutDownSignal, storageLinksCache, metadataCache, storageAlgRef)
}

final case class PreshutdownServiceConfig(
    storageLinksJsonBlobName: CloudStorageBlob,
    gcsMetadataJsonBlobName: CloudStorageBlob,
    workingDirectory: Path,
    stagingBucketName: CloudStorageContainer
)
