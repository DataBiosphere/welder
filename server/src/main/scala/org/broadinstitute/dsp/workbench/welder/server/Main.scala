package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path

import cats.effect.concurrent.Ref
import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import fs2.Stream
import io.chrisdavenport.linebacker.Linebacker
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Printer}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.ExecutionContext

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val unsafeLogger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
      blockingEc <- Stream.resource[IO, ExecutionContext](ExecutionContexts.fixedThreadPool(255))
      implicit0(it: Linebacker[IO])  = Linebacker.fromExecutionContext[IO](blockingEc)
      appConfig <- Stream.fromEither[IO](Config.appConfig)
      storageLinksCache <- cachedResource[LocalDirectory, StorageLink](appConfig.pathToStorageLinksJson, blockingEc, storageLink => List(storageLink.localBaseDirectory -> storageLink, storageLink.localSafeModeBaseDirectory -> storageLink))
      metadataCache <- cachedResource[Path, GcsMetadata](appConfig.pathToGcsMetadataJson, blockingEc, metadata => List(metadata.localPath -> metadata))
      googleStorageService <- Stream.resource(GoogleStorageService.fromApplicationDefault())
      storageLinksService = StorageLinksService(storageLinksCache)
      objectServiceConfig = ObjectServiceConfig(appConfig.workingDirectory, appConfig.currentUser, appConfig.lockExpiration)
      objectService = ObjectService(objectServiceConfig, googleStorageService, blockingEc, storageLinksCache, metadataCache)
      server <- BlazeServerBuilder[IO].bindHttp(8080, "0.0.0.0").withHttpApp(WelderApp(objectService, storageLinksService).service).serve
    } yield ()

    app.handleErrorWith {
        error =>
          Stream.eval(Logger[IO].error(error)("Failed to start server")) >> Stream.raiseError[IO](error)
      }
      .evalMap(_ => IO.never)
      .compile
      .drain
      .as(ExitCode.Success)
  }

  private def cachedResource[A, B: Decoder: Encoder](path: Path, blockingEc: ExecutionContext, toTuple: B => List[(A, B)])(implicit logger: Logger[IO]): Stream[IO, Ref[IO, Map[A, B]]] = for {
    cached <- readJsonFileToA[IO, List[B]](path).map(ls => ls.flatMap(b => toTuple(b)).toMap).handleErrorWith { error =>
      Stream.eval(logger.info(s"$path not found")) >> Stream.emit(Map.empty[A, B]).covary[IO]
    }
    ref <- Stream.resource(Resource.make(Ref.of[IO, Map[A, B]](cached))(
      ref => Stream.eval(ref.get).flatMap(x => Stream.emits(x.values.toSet.asJson.pretty(Printer.noSpaces).getBytes("UTF-8")))
        .through(fs2.io.file.writeAll(path, blockingEc))
        .compile
        .drain
    ))
  } yield ref

}