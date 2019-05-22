package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.concurrent.Ref
import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.parser.decode
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.http4s.server.blaze.BlazeServerBuilder
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import io.circe.syntax._

import scala.concurrent.ExecutionContext
import scala.util.Try

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val unsafeLogger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
      appConfig <- Stream.fromEither[IO](Config.appConfig)
      blockingEc <- Stream.resource[IO, ExecutionContext](ExecutionContexts.fixedThreadPool(255))
      storageLinksCache <- Stream.resource[IO, Ref[IO, Map[LocalDirectory, StorageLink]]](Resource.make(Ref.of[IO, Map[LocalDirectory, StorageLink]](Map.empty))(x => x.get.map(y => reflect.io.File("storagelinks.json").writeAll(y.values.toSet.asJson.toString))))
      googleStorageService <- Stream.resource(GoogleStorageService.resource(appConfig.pathToGoogleStorageCredentialJson, blockingEc))
      storageLinksService = StorageLinksService(storageLinksCache)
      syncService = ObjectService(googleStorageService)
      server <- BlazeServerBuilder[IO].bindHttp(8080, "0.0.0.0").withHttpApp(WelderApp(syncService, storageLinksService).service).serve
    } yield ()

    app
      .handleErrorWith(error => Stream.eval(Logger[IO].error(error)("Failed to start server")))
      .evalMap(_ => IO.never)
      .compile
      .drain
      .as(ExitCode.Success)
  }

  private val storageLinksFileName = "storageLinks.json" //todo: add to config

  private def loadStorageLinks() = {
    val storageLinksString = Try(scala.io.Source.fromFile("storagelinks.json").mkString).recover {
      case _ => ""
    }.get

    val storageLinks = decode[Set[StorageLink]](storageLinksString) match {
      case Left(_) => IO.pure(Map[LocalDirectory, StorageLink]()) //TODO: actually handle error here
      case Right(foo) => IO.pure(foo.map { x => x.localBaseDirectory -> x}.toMap)
    }

    storageLinks.map { foo =>
      Resource.make(
        Ref.of[IO, Map[LocalDirectory, StorageLink]](foo)
      )(x => x.get.map(y => reflect.io.File("storagelinks.json").writeAll(y.values.toSet.asJson.toString)))
    }

  }

  private def persistStorageLinks() = {

  }
}
