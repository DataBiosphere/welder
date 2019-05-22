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
import StorageLinksService._

import scala.concurrent.ExecutionContext
import scala.util.Try

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val unsafeLogger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
      appConfig <- Stream.fromEither[IO](Config.appConfig)
      blockingEc <- Stream.resource[IO, ExecutionContext](ExecutionContexts.fixedThreadPool(255))
      storageLinksCache <- Stream.resource[IO, Ref[IO, Map[LocalDirectory, StorageLink]]](Resource.make(Ref.of[IO, Map[LocalDirectory, StorageLink]](initializeStorageLinks(appConfig.pathToStorageLinksJson)))(persistStorageLinks(appConfig.pathToStorageLinksJson)))
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

  private def initializeStorageLinks(pathToStorageLinksJson: String): Map[LocalDirectory, StorageLink] = {
    val storageLinksString = Try(scala.io.Source.fromFile(pathToStorageLinksJson).mkString).recover {
      case _ => ""
    }.get

    decode[Set[StorageLink]](storageLinksString) match {
      case Left(_) => Map[LocalDirectory, StorageLink]() //TODO: actually handle error here
      case Right(storageLinks) => storageLinks.map { link => link.localBaseDirectory -> link}.toMap
    }
  }

  private def persistStorageLinks(pathToStorageLinksJson: String) = {
    storageLinks: Ref[IO, Map[LocalDirectory, StorageLink]] =>
      storageLinks.get.map(linksMap => reflect.io.File(pathToStorageLinksJson).writeAll(linksMap.values.toSet.asJson.toString))
  }
}
