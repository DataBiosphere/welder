package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path
import java.util.UUID
import cats.effect.{Blocker, ContextShift, IO}
import cats.implicits._
import cats.mtl.Ask
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

// This endpoint is called by leonardo before it tells dataproc to shut down user's vm.
class ShutdownService(
    config: PreshutdownServiceConfig,
    shutDownSignal: SignallingRef[IO, Boolean],
    storageLinksCache: StorageLinksCache,
    metadataCache: MetadataCache,
    googleStorageAlg: GoogleStorageAlg,
    blocker: Blocker
)(implicit cs: ContextShift[IO], logger: Logger[IO])
    extends Http4sDsl[IO] {

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case POST -> Root / "flush" =>
      flush >> NoContent()
  }

  val flush: IO[Unit] = {
    val flushStorageLinks = flushCache(googleStorageAlg, config.stagingBucketName, config.storageLinksJsonBlobName, storageLinksCache)
    val flushMetadataCache = flushCache(googleStorageAlg, config.stagingBucketName, config.gcsMetadataJsonBlobName, metadataCache)

    // Copy all welder log files and jupyter log file to staging bucket
    val flushLogFiles = for {
      implicit0(ev: Ask[IO, TraceId]) <- IO(Ask.const[IO, TraceId](TraceId(UUID.randomUUID().toString)))
      _ <- findFilesWithSuffix(config.workingDirectory, ".log").traverse_ { file =>
        val blobName = GcsBlobName(s"cluster-log-files/${file.getName}")
        googleStorageAlg.fileToGcsAbsolutePath(file.toPath, GsPath(config.stagingBucketName, blobName))
      }
    } yield ()

    val streams = flushStorageLinks ++ flushMetadataCache ++ Stream.eval(flushLogFiles)
    Logger[IO].info("Shutting down welder") >> Stream(streams)
      .parJoin(3)
      .compile
      .drain >> IO(shutDownSignal.update(_ => true)).void //shut down http server
  }
}

object ShutdownService {
  def apply(
      config: PreshutdownServiceConfig,
      shutDownSignal: SignallingRef[IO, Boolean],
      storageLinksCache: StorageLinksCache,
      metadataCache: MetadataCache,
      googleStorageAlg: GoogleStorageAlg,
      blocker: Blocker
  )(
      implicit cs: ContextShift[IO],
      logger: Logger[IO]
  ): ShutdownService = new ShutdownService(config, shutDownSignal, storageLinksCache, metadataCache, googleStorageAlg, blocker)
}

final case class PreshutdownServiceConfig(
    storageLinksJsonBlobName: GcsBlobName,
    gcsMetadataJsonBlobName: GcsBlobName,
    workingDirectory: Path,
    stagingBucketName: GcsBucketName
)
