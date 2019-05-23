package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path
import java.time.Instant
import java.util.UUID.randomUUID

import _root_.fs2.{Stream, io}
import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.circe.{Decoder, Encoder}
import ca.mrvisser.sealerate
import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.server.ObjectService._
import org.broadinstitute.dsp.workbench.welder.server.PostObjectRequest._
import org.broadinstitute.dsp.workbench.welder.server.StorageLink.storageLinkEncoder
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Uri}

import scala.concurrent.ExecutionContext

class ObjectService(googleStorageService: GoogleStorageService[IO], blockingEc: ExecutionContext, pathToStorageLinks: Path)(implicit cs: ContextShift[IO], logger: Logger[IO]) extends Http4sDsl[IO] {
  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ GET -> Root / "metadata" =>
      for {
        metadataReq <- req.as[GetMetadataRequest]
        resp <- Ok(checkMetadata(metadataReq))
      } yield resp
    case req @ POST -> Root =>
      for {
        localizeReq <- req.as[PostObjectRequest]
        res <- localizeReq match {
          case x: Localize => localize(x)
          case x: SafeDelocalize => safeDelocalize(x)
        }
        resp <- Ok(res)
      } yield resp
  }

  def localize(req: Localize): IO[Unit] = {
    val res = Stream.emits(req.entries).map {
      entry =>
        googleStorageService.getObject(entry.bucketNameAndObjectName.bucketName, entry.bucketNameAndObjectName.blobName, None) //get file from google. TODO: pass generation when storing object
          .through(io.file.writeAll(entry.localObjectPath, blockingEc)) //write file to local disk
    }.parJoin(10)

    res.compile.drain
  }

  def checkMetadata(req: GetMetadataRequest): IO[MetadataResponse] =
    for {
      traceId <- IO(TraceId(randomUUID()))
      storageLinks <- readJsonFileToA[IO, List[StorageLink]](pathToStorageLinks).compile.lastOrError
      storageLink = storageLinks.find(_.localBaseDirectory == req.localObjectPath) //TODO: handle recursive flag
      isLinked = storageLink.nonEmpty
      res <- storageLink.fold[IO[MetadataResponse]](IO.raiseError(StorageLinkNotFoundException(s"No storage link found for ${req.localObjectPath}"))) {
        sl =>
          for {
            bucketAndObject <- IO.fromEither(parseGsDirectory(sl.cloudStorageDirectory.renderString).leftMap(e => InternalException(e)))
            metadata <- googleStorageService.getObjectMetadata(bucketAndObject.bucketName, bucketAndObject.blobName, Some(traceId)).compile.lastOrError
            result <- metadata match {
                        case google2.GetMetadataResponse.NotFound =>
                          IO.pure(MetadataResponse(isLinked, SyncStatus.RemoteNotFound, None, None, true, sl)) //TODO: fix isExecutionMode
                        case google2.GetMetadataResponse.Metadata(crc32c, userDefinedMetadata) =>
                          for {
                            fileBody <- io.file.readAll[IO](sl.localBaseDirectory, blockingEc, 4096).compile.to[Array]
                            calculatedCrc32c = Crc32c.calculateCrc32c(fileBody)
                            syncStatus = if (calculatedCrc32c == crc32c) SyncStatus.Live
                              else SyncStatus.Desynchronized
                            lastEditedBy = userDefinedMetadata.get("lastEditedBy").map(WorkbenchEmail)
                            lastEditedTime <- userDefinedMetadata.get("lastEditedTime").flatTraverse[IO, Instant] {
                              str =>
                                Either.catchNonFatal(str.toLong) match {
                                  case Left(t) => logger.warn(s"Failed to convert $str to epoch millis") *> IO.pure(None)
                                  case Right(s) => IO.pure(Some(Instant.ofEpochMilli(s)))
                                }
                            }
                          } yield MetadataResponse(isLinked, syncStatus, lastEditedBy, lastEditedTime, true, sl) //TODO: fix isExecutionMode
                      }
          } yield result
      }
    } yield res

  def safeDelocalize(req: SafeDelocalize): IO[Unit] = {
    for {
      traceId <- IO(TraceId(randomUUID()))
      storageLinks <- readJsonFileToA[IO, List[StorageLink]](pathToStorageLinks).compile.lastOrError
      storageLink = storageLinks.find(_.localBaseDirectory == req.localObjectPath)
      _ <- storageLink.fold[IO[Unit]](IO.raiseError(StorageLinkNotFoundException(s"No storage link found for ${req.localObjectPath}"))) {
        sl =>
          for {
            metadata <- checkMetadata(GetMetadataRequest(req.localObjectPath))
            _ <- metadata match {
              case meta: MetadataResponse =>
                for {
                  _ <- if(meta.syncStatus == SyncStatus.Live) {
                    delocalize(req.localObjectPath, sl.cloudStorageDirectory, traceId)
                  } else {
                    val msg = s"File is outdated(${meta.syncStatus}), hence not delocalizing"
                    logger.info(msg) *> IO.raiseError(FileOutOfSyncException(msg))
                  }
                } yield ()
            }
          } yield ()
      }
    } yield ()
  }

  private def delocalize(localObjectPath: java.nio.file.Path, gsPath: Uri, traceId: TraceId): IO[Unit] = io.file.readAll[IO](localObjectPath, blockingEc, 4096).compile.to[Array].flatMap {
    body =>
      for {
        bucketNameAndObjectName <- IO.fromEither(parseGsDirectory(gsPath.renderString).leftMap(InternalException))
        _ <- googleStorageService.storeObject(bucketNameAndObjectName.bucketName, bucketNameAndObjectName.blobName, body, gcpObjectType, Some(traceId))
      } yield ()
  }
}

object ObjectService {
  def apply(googleStorageService: GoogleStorageService[IO], blockingEc: ExecutionContext, pathToStorageLinks: Path)(implicit cs: ContextShift[IO], logger: Logger[IO]): ObjectService = new ObjectService(googleStorageService, blockingEc, pathToStorageLinks)

  val gcpObjectType = "text/plain"

  implicit val actionDecoder: Decoder[Action] = Decoder.decodeString.emap {
    str =>
      Action.stringToAction.get(str).toRight("invalid action")
  }

  implicit val entryDecoder: Decoder[Entry] = Decoder.instance {
    cursor =>
      for {
        bucketAndObject <- cursor.downField("sourceUri").as[BucketNameAndObjectName]
        localObjectPath <- cursor.downField("localDestinationPath").as[Path]
      } yield Entry(bucketAndObject, localObjectPath)
  }

  implicit val localizeDecoder: Decoder[Localize] = Decoder.forProduct1("entries"){
    Localize.apply
  }

  implicit val safeDelocalizeDecoder: Decoder[SafeDelocalize] = Decoder.forProduct1("localPath"){
    SafeDelocalize.apply
  }

  implicit val postObjectRequestDecoder: Decoder[PostObjectRequest] = Decoder.instance {
    cursor =>
      for {
        action <- cursor.downField("action").as[Action]
        req <- action match {
          case Action.Localize =>
            cursor.as[Localize]
          case Action.SafeDelocalize =>
            cursor.as[SafeDelocalize]
        }
      } yield req
  }

  implicit val getMetadataDecoder: Decoder[GetMetadataRequest] = Decoder.forProduct1("localPath")(GetMetadataRequest.apply)


  implicit val metadataResponseEncoder: Encoder[MetadataResponse] = Encoder.forProduct6(
    "isLinked",
    "syncStatus",
    "lastEditedBy",
    "lastEditedTime",
    "isExecutionMode",
    "storageLink"
  )(x => MetadataResponse.unapply(x).get)
}

final case class GetMetadataRequest(localObjectPath: Path)
sealed abstract class Action
object Action {
  final case object Localize extends Action {
    override def toString: String = "localize"
  }
  final case object SafeDelocalize extends Action {
    override def toString: String = "safeDelocalize"
  }

  val stringToAction: Map[String, Action] = sealerate.values[Action].map(a => a.toString -> a).toMap
}
sealed abstract class PostObjectRequest extends Product with Serializable {
  def action: Action
}
object PostObjectRequest {
  final case class Localize(entries: List[Entry]) extends PostObjectRequest {
    override def action: Action = Action.Localize
  }
  final case class SafeDelocalize(localObjectPath: Path) extends PostObjectRequest {
    override def action: Action = Action.SafeDelocalize
  }
}

final case class Entry(bucketNameAndObjectName: BucketNameAndObjectName, localObjectPath: Path)

final case class LocalizeRequest(entries: List[Entry])

final case class MetadataResponse(
                        isLinked: Boolean,
                        syncStatus: SyncStatus,
                        lastEditedBy: Option[WorkbenchEmail],
                        lastEditedTime: Option[Instant],
                        isExecutionMode: Boolean,
                        storageLinks: StorageLink
                      )