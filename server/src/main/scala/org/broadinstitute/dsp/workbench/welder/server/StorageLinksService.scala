package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.{Blocker, ContextShift, IO}
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
import _root_.org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.model.TraceId

class StorageLinksService(
    storageLinks: StorageLinksCache,
    googleStorageAlg: GoogleStorageAlg,
    metadataCacheAlg: MetadataCacheAlg,
    blocker: Blocker,
    config: StorageLinksServiceConfig
)(
    implicit logger: StructuredLogger[IO],
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
        val safeModeDirectory =
          storageLink.localSafeModeBaseDirectory.fold[List[Tuple2[RelativePath, StorageLink]]](List.empty)(l => List(l.path -> storageLink))

        val toAdd = List(storageLink.localBaseDirectory.path -> storageLink).toMap ++ safeModeDirectory
        (links ++ toAdd, storageLink)
      }
      _ <- initializeDirectories(storageLink)
      _ <- persistWorkspaceBucket(link.localBaseDirectory, link.localSafeModeBaseDirectory, link.cloudStorageDirectory)
      _ <- (googleStorageAlg
        .localizeCloudDirectory(storageLink.localBaseDirectory.path, storageLink.cloudStorageDirectory, config.workingDirectory, storageLink.pattern, traceId)
        .through(metadataCacheAlg.updateCachePipe))
        .compile
        .drain
        .runAsync { cb =>
          cb match {
            case Left(e) =>
              logger.warn(Map("traceId" -> traceId.asString), e)(s"fail to download files under ${storageLink.cloudStorageDirectory} when creating storagelink")
            case Right(()) => IO.unit
          }
        }
        .toIO
    } yield link
  }

  private def persistWorkspaceBucket(
      baseDirectory: LocalDirectory,
      safeModeDirectory: Option[LocalDirectory],
      cloudStorageDirectory: CloudStorageDirectory
  ): IO[Unit] = {
    val fileBody =
      RuntimeVariables(
        s"gs://${cloudStorageDirectory.bucketName.value}/notebooks"
      ) //appending notebooks to mimick old jupyter image until we start using new images.
      .asJson.printWith(Printer.noSpaces).getBytes(Charset.`UTF-8`.toString())
    val editModeDestinationPath = config.workingDirectory
      .resolve(baseDirectory.path.asPath)
      .resolve(config.workspaceBucketNameFileName)
    val safeModeDestinationPath = safeModeDirectory.map(d =>
      config.workingDirectory
        .resolve(d.path.asPath)
        .resolve(config.workspaceBucketNameFileName)
    )

    val writeToFile: java.nio.file.Path => IO[Unit] = destinationPath =>
      logger.info(s"writing ${destinationPath}") >> (Stream.emits(fileBody) through io.file.writeAll[IO](
        destinationPath,
        blocker,
        List(StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
      )).compile.drain // overwrite the file everytime storagelink is called since workspace bucket can be updated

    (writeToFile(editModeDestinationPath), safeModeDestinationPath.traverse(p => writeToFile(p))).parTupled.void
  }

  //returns whether the directories exist at the end of execution
  private def initializeDirectories(storageLink: StorageLink): IO[Unit] = {
    val localSafeAbsolutePath = storageLink.localSafeModeBaseDirectory.map(d => config.workingDirectory.resolve(d.path.asPath))
    val localEditAbsolutePath = config.workingDirectory.resolve(storageLink.localBaseDirectory.path.asPath)

    val dirsToCreate: List[java.nio.file.Path] = List(localEditAbsolutePath) ++ localSafeAbsolutePath

    //creates all dirs and logs any errors
    dirsToCreate.traverse { dir =>
      val file = dir.toFile()
      for {
        exists <- IO(file.exists())
        _ <- if (exists) IO.unit
        else {
          IO(file.mkdirs()).flatMap { r =>
            if (!r) logger.warn(s"could not initialize dir ${file.getPath()}")
            else logger.info(s"Successfully created ${file}")
          }
        }
      } yield ()
    }.void
  }

  def deleteStorageLink(storageLink: StorageLink): IO[Unit] =
    storageLinks.modify { links =>
      val toDelete = List(storageLink.localBaseDirectory.path) ++ storageLink.localSafeModeBaseDirectory.map(_.path)
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
  def apply(
      storageLinks: StorageLinksCache,
      googleStorageAlg: GoogleStorageAlg,
      metadataCacheAlg: MetadataCacheAlg,
      config: StorageLinksServiceConfig,
      blocker: Blocker
  )(
      implicit logger: StructuredLogger[IO],
      contextShift: ContextShift[IO]
  ): StorageLinksService =
    new StorageLinksService(storageLinks, googleStorageAlg, metadataCacheAlg, blocker, config)

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
