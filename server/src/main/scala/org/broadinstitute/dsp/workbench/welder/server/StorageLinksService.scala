package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.{ContextShift, IO}
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.server.StorageLinksService._
import org.http4s.{Charset, HttpRoutes}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import java.nio.file.{Path, StandardOpenOption}

import fs2.{Stream, io}
import cats.data.Kleisli
import cats.implicits._
import _root_.io.circe.Encoder.encodeString
import _root_.io.circe.Printer
import _root_.io.circe.syntax._
import _root_.io.chrisdavenport.linebacker.Linebacker
import _root_.io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.model.TraceId

class StorageLinksService(storageLinks: StorageLinksCache, googleStorageAlg: GoogleStorageAlg, metadataCacheAlg: MetadataCacheAlg, config: StorageLinksServiceConfig)(
    implicit logger: Logger[IO],
    linerBacker: Linebacker[IO],
    contextShift: ContextShift[IO]
) extends WelderService {
  val service: HttpRoutes[IO] = withTraceId {
    case GET -> Root =>
      _ =>
        for {
          res <- getStorageLinks
          resp <- Ok(res)
        } yield resp
    case req @ DELETE -> Root =>
      _ =>
        for {
          storageLink <- req.as[StorageLink]
          _ <- deleteStorageLink(storageLink)
          resp <- NoContent()
        } yield resp
    case req @ POST -> Root =>
      traceId =>
        for {
          storageLink <- req.as[StorageLink]
          res <- createStorageLink(storageLink).run(traceId)
          resp <- Ok(res)
        } yield resp
  }

  //note: first param in the modify is the thing to do, second param is the value to return
  def createStorageLink(storageLink: StorageLink): Kleisli[IO, TraceId, StorageLink] = Kleisli { traceId =>
    for {
      link <- storageLinks.modify { links =>
        val toAdd = List(storageLink.localBaseDirectory.path -> storageLink, storageLink.localSafeModeBaseDirectory.path -> storageLink).toMap
        (links ++ toAdd, storageLink)
      }
      _ <-  persistWorkspaceBucket(link.localBaseDirectory, link.cloudStorageDirectory)
      _ <- initializeDirectories(storageLink)
      _ <- (googleStorageAlg
        .localizeCloudDirectory(storageLink.localBaseDirectory.path, storageLink.cloudStorageDirectory, config.workingDirectory, storageLink.pattern, traceId)
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
  }

  private def persistWorkspaceBucket(baseDirectory: LocalDirectory, cloudStorageDirectory: CloudStorageDirectory): IO[Unit] = {
    val fileBody = RuntimeVariables("gs://"+cloudStorageDirectory.bucketName.value).asJson.pretty(Printer.noSpaces).getBytes(Charset.`UTF-8`.toString())
    val destinationPath = config.workingDirectory.resolve(baseDirectory.path.asPath).resolve(config.workspaceBucketNameFileName)
    ((Stream.emits(fileBody) through io.file.writeAll[IO](destinationPath, linerBacker.blockingContext, List(StandardOpenOption.CREATE_NEW))).compile.drain)
      .recoverWith{ case _ => logger.info(s"${config.workspaceBucketNameFileName} already exists")} // If file already exists, ignore the failure
  }

  //returns whether the directories exist at the end of execution
  private def initializeDirectories(storageLink: StorageLink): IO[Unit] = {
    val localSafeAbsolutePath = config.workingDirectory.resolve(storageLink.localSafeModeBaseDirectory.path.asPath)
    val localEditAbsolutePath = config.workingDirectory.resolve(storageLink.localBaseDirectory.path.asPath)

    val dirsToCreate: List[java.nio.file.Path] = List[java.nio.file.Path](localSafeAbsolutePath, localEditAbsolutePath)

    val files = dirsToCreate
      .map(path => new java.io.File(path.toUri))

    //creates all dirs and logs any errors
    files.traverse { file =>
      for {
        exists <- IO(file.exists())
        _ <- if (exists) IO.unit
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

final case class RuntimeVariables(workspaceBucket: String)

final case class StorageLinks(storageLinks: Set[StorageLink])
final case class StorageLinksServiceConfig(workingDirectory: Path, workspaceBucketNameFileName: Path)
object StorageLinksService {
  def apply(storageLinks: StorageLinksCache, googleStorageAlg: GoogleStorageAlg, metadataCacheAlg: MetadataCacheAlg, config: StorageLinksServiceConfig)(
      implicit logger: Logger[IO],
      linebacker: Linebacker[IO],
      contextShift: ContextShift[IO]
  ): StorageLinksService =
    new StorageLinksService(storageLinks, googleStorageAlg, metadataCacheAlg, config)

  implicit val storageLinksEncoder: Encoder[StorageLinks] = Encoder.forProduct1(
    "storageLinks"
  )(storageLinks => StorageLinks.unapply(storageLinks).get)

  implicit val storageLinksDecoder: Decoder[StorageLinks] = Decoder.forProduct1(
    "storageLinks"
  )(StorageLinks.apply)

  implicit val runtimeVariablesEncoder: Encoder[RuntimeVariables] = Encoder.forProduct1(
    "destination"
  )(x => RuntimeVariables.unapply(x).get)
}
