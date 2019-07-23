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
import java.util.UUID

import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.model.TraceId

class StorageLinksService(storageLinks: StorageLinksCache, googleStorageAlg: GoogleStorageAlg, metadataCacheAlg: MetadataCacheAlg, workingDirectory: Path)(
    implicit logger: Logger[IO]
) extends Http4sDsl[IO] {
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
        resp <- NoContent()
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
    for {
      traceId <- IO(TraceId(UUID.randomUUID()))
      link <- storageLinks.modify { links =>
        val toAdd = List(storageLink.localBaseDirectory.path -> storageLink, storageLink.localSafeModeBaseDirectory.path -> storageLink).toMap
        (links ++ toAdd, storageLink)
      }
      _ <- initializeDirectories(storageLink)
      _ <- (googleStorageAlg
        .localizeCloudDirectory(storageLink.localBaseDirectory.path, storageLink.cloudStorageDirectory, workingDirectory, traceId)
        .through(metadataCacheAlg.updateCachePipe))
        .compile
        .drain
        .runAsync { cb =>
          cb match {
            case Left(r) => logger.warn(s"fail to download files under ${storageLink.cloudStorageDirectory} when creating storagelink")
            case Right(()) => IO.unit
          }
        }
        .toIO
    } yield link

  //returns whether the directories exist at the end of execution
  def initializeDirectories(storageLink: StorageLink): IO[Unit] = {
    val localSafeAbsolutePath = workingDirectory.resolve(storageLink.localSafeModeBaseDirectory.path.asPath)
    val localEditAbsolutePath = workingDirectory.resolve(storageLink.localBaseDirectory.path.asPath)

    val dirsToCreate: List[java.nio.file.Path] = List[java.nio.file.Path](localSafeAbsolutePath, localEditAbsolutePath)

    val files = dirsToCreate
      .map(path => new java.io.File(path.toUri))

    //creates all dirs and logs any errors
    files.traverse { file =>
      for {
        exists <- IO(file.exists())
        canDirBeMade <- if (exists) IO.unit
        else {
          IO(file.mkdir()).flatMap(r => {
            if (!r) logger.warn(s"could not initialize dir ${file.getPath()}")
            else IO.unit
          })
        }
      } yield ()
    }.void
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
  def apply(storageLinks: StorageLinksCache, googleStorageAlg: GoogleStorageAlg, metadataCacheAlg: MetadataCacheAlg, workingDirectory: Path)(
      implicit logger: Logger[IO]
  ): StorageLinksService =
    new StorageLinksService(storageLinks, googleStorageAlg, metadataCacheAlg, workingDirectory)

  implicit val storageLinksEncoder: Encoder[StorageLinks] = Encoder.forProduct1(
    "storageLinks"
  )(storageLinks => StorageLinks.unapply(storageLinks).get)

  implicit val storageLinksDecoder: Decoder[StorageLinks] = Decoder.forProduct1(
    "storageLinks"
  )(StorageLinks.apply)
}
