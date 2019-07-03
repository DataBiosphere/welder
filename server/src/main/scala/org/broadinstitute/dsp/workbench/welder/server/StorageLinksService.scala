package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.IO
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.server.StorageLinksService._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import java.nio.file.Path

import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

class StorageLinksService(storageLinks: StorageLinksCache, workingDirectory: Path) extends Http4sDsl[IO] {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root =>
      for {
        res <- getStorageLinks
        resp <- Ok(res)
      } yield resp
    case req @ DELETE -> Root =>
      for {
        storageLink <- req.as[StorageLink]
        _ <- deleteStorageLink(storageLink)
        resp <- Ok(())
      } yield resp
    case req @ POST -> Root =>
      for {
        storageLink <- req.as[StorageLink]
        res <- createStorageLink(storageLink)
        resp <- Ok(res)
      } yield resp
  }

  //note: first param in the modify is the thing to do, second param is the value to return
  def createStorageLink(storageLink: StorageLink): IO[StorageLink] =
    storageLinks.modify { links =>

      val toAdd = List(storageLink.localBaseDirectory.path -> storageLink, storageLink.localSafeModeBaseDirectory.path -> storageLink).toMap

      initializeDirectories(storageLink)

      (links ++ toAdd, storageLink)
    }

  //returns whether the directories exist at the end of execution
  def initializeDirectories(storageLink: StorageLink): Boolean = {
    val localSafeAbsolutePath = workingDirectory.resolve(storageLink.localSafeModeBaseDirectory.path.asPath)
    val localEditAbsolutePath = workingDirectory.resolve(storageLink.localBaseDirectory.path.asPath)

    val dirsToCreate: List[java.nio.file.Path] = List[java.nio.file.Path](localSafeAbsolutePath, localEditAbsolutePath)

    dirsToCreate
      .map(path => new java.io.File(path.toUri))
      .map(dir => dir.exists() || dir.mkdir()) //mkdir returns true if the directory is created. if it exists it returns false.
      .foldRight(true)(_ && _)
  }

  def deleteStorageLink(storageLink: StorageLink): IO[Unit] =
    storageLinks.modify { links =>
      val toDelete = List(storageLink.localBaseDirectory.path, storageLink.localSafeModeBaseDirectory.path)
      (links -- toDelete, ())
    }

  val getStorageLinks: IO[StorageLinks] = {
    storageLinks.get.map(links => StorageLinks(links.values.toSet))
  }
}

final case class StorageLinks(storageLinks: Set[StorageLink])

object StorageLinksService {
  def apply(storageLinks: StorageLinksCache, workingDirectory: Path): StorageLinksService = new StorageLinksService(storageLinks, workingDirectory)

  implicit val storageLinksEncoder: Encoder[StorageLinks] = Encoder.forProduct1(
    "storageLinks"
  )(storageLinks => StorageLinks.unapply(storageLinks).get)

  implicit val storageLinksDecoder: Decoder[StorageLinks] = Decoder.forProduct1(
    "storageLinks"
  )(StorageLinks.apply)
}
