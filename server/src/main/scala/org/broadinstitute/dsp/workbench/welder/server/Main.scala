package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.ExecutionContext

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val unsafeLogger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
//      appConfig <- Stream.fromEither[IO](Config.appConfig)
      blockingEc <- Stream.resource[IO, ExecutionContext](ExecutionContexts.fixedThreadPool(255))
//      googleStorageService <- Stream.resource(GoogleStorageService.resource(appConfig.pathToGoogleStorageCredentialJson, blockingEc))
      syncService = ObjectService(null, blockingEc)
      server <- BlazeServerBuilder[IO].bindHttp(8080, "0.0.0.0").withHttpApp(WelderApp(syncService).service).serve
    } yield ()

    app
      .handleErrorWith(error => Stream.eval(Logger[IO].error(error)("Failed to start server")))
      .evalMap(_ => IO.never)
      .compile
      .drain
      .as(ExitCode.Success)
  }
}