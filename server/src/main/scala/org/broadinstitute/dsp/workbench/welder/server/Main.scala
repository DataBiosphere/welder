package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path

import cats.effect.concurrent.Ref
import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Printer
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import StorageLinksService._
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.ExecutionContext

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val unsafeLogger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
      appConfig <- Stream.fromEither[IO](Config.appConfig)
      blockingEc <- Stream.resource[IO, ExecutionContext](ExecutionContexts.fixedThreadPool(255))
      storageLinks <- readJsonFileToA[IO, StorageLinks](appConfig.pathToStorageLinksJson).map(sl => sl.storageLinks.map(s => s.localBaseDirectory -> s).toMap)
      storageLinksCache <- Stream.resource[IO, Ref[IO, Map[Path, StorageLink]]](Resource.make(Ref.of[IO, Map[Path, StorageLink]](storageLinks))(
        ref => Stream.eval(ref.get).flatMap(x => Stream.emits(StorageLinks(x.values.toSet).asJson.pretty(Printer.noSpaces).getBytes("UTF-8")))
          .through(fs2.io.file.writeAll(appConfig.pathToStorageLinksJson, blockingEc))
          .compile
          .drain
      ))
      googleStorageService <- Stream.resource(GoogleStorageService.resource(appConfig.pathToGoogleStorageCredentialJson, blockingEc))
      storageLinksService = StorageLinksService(storageLinksCache)
      objectService = ObjectService(googleStorageService, blockingEc, appConfig.pathToStorageLinksJson)
      server <- BlazeServerBuilder[IO].bindHttp(8080, "0.0.0.0").withHttpApp(WelderApp(objectService, storageLinksService).service).serve
    } yield ()

    app
      .handleErrorWith(error => Stream.eval(Logger[IO].error(error)("Failed to start server")))
      .evalMap(_ => IO.never)
      .compile
      .drain
      .as(ExitCode.Success)
  }
}