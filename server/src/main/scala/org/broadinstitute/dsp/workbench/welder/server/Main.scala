package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.http4s.server.blaze.BlazeServerBuilder

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =  {
    implicit val unsafeLogger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
      appConfig <- Stream.fromEither[IO](Config.appConfig)
      server <- BlazeServerBuilder[IO].bindHttp(8080, "0.0.0.0").withHttpApp(WelderApp.apply.service).serve
    } yield ()

    app.handleErrorWith(error => Stream.emit(Logger[IO].error(error)("Failed to start server")))
      .evalMap(_ => IO.never)
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
