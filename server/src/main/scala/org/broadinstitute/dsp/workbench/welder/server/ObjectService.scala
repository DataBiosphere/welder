package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.{Path, Paths}
import java.time.Instant
import java.util.UUID.randomUUID

import cats.implicits._
import ca.mrvisser.sealerate
import cats.effect.{ContextShift, IO}
import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.circe.{Decoder, Encoder}
import fs2.{Stream, io}
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.server.ObjectService._
import org.broadinstitute.dsp.workbench.welder.server.PostObjectRequest._
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
        googleStorageService.getObject(entry.bucketNameAndObjectName.bucketName, entry.bucketNameAndObjectName.blobName, None) //get file from google
          .through(io.file.writeAll(Paths.get(entry.localObjectPath.asString), blockingEc)) //write file to local disk
    }.parJoin(10)

    res.compile.drain
  }

  def checkMetadata(req: GetMetadataRequest): IO[MetadataResponse] =
    for {
      traceId <- IO(TraceId(randomUUID()))
      storageLinks <- readJsonFileToA[IO, List[StorageLink]](pathToStorageLinks).compile.lastOrError
      storageLink = storageLinks.find(_.localBaseDirectory == req.localObjectPath)
      res <- storageLink.fold[IO[MetadataResponse]](IO.raiseError(NotFoundException(s"No storage link found for ${req.localObjectPath}"))) {
        sl =>
          for {
            bucketAndObject <- IO.fromEither(JsonCodec.parseGsDirectory(sl.cloudStorageDirectory.renderString).leftMap(e => InternalException(e)))
            metadata <- googleStorageService.getObjectMetadata(bucketAndObject.bucketName, bucketAndObject.blobName, Some(traceId)).compile.lastOrError
            result <- metadata match {
                        case google2.GetMetadataResponse.NotFound => IO.raiseError(NotFoundException(s"Object(${bucketAndObject}) not found in Google storage"))
                        case google2.GetMetadataResponse.Metadata(crc32, userDefinedMetadata) =>
                          for {
                            isLinkedString <- IO.fromEither(userDefinedMetadata.get("isLinked").toRight(NotFoundException("isLinked key missing from object metadata ")))
                            isLinked <- IO(isLinkedString.toBoolean)
                            fileBody <- googleStorageService.getObject(bucketAndObject.bucketName, bucketAndObject.blobName, Some(traceId)).compile.to[Array]
                            calculatedCrc32 = Crc32c.calculateCrc32c(fileBody)
                            syncStatus = if (fileBody.length == 0) SyncStatus.DeletedRemote
                              else if (calculatedCrc32 == crc32) SyncStatus.Live
                              else SyncStatus.Desynchronized
                            lastEditedBy <- IO.fromEither(userDefinedMetadata.get("lastEditedBy").map(WorkbenchEmail).toRight(NotFoundException("lastEditedBy key missing from object metadata ")))
                            lastEditedTimeString <- IO.fromEither(userDefinedMetadata.get("lastEditedTime").toRight(NotFoundException("lastEditedTime key missing from object metadata ")))
                            lastEditedTime <- IO(Instant.ofEpochMilli(lastEditedBy.value.toLong))
                          } yield MetadataResponse(isLinked, syncStatus, lastEditedBy, lastEditedTime, sl.cloudStorageDirectory, true, sl) //TODO: fix isExecutionMode
                      }
          } yield result
      }
    } yield res

  def safeDelocalize(req: SafeDelocalize): IO[Unit] = {
    for {
      traceId <- IO(TraceId(randomUUID()))
      storageLinks <- readJsonFileToA[IO, List[StorageLink]](pathToStorageLinks).compile.lastOrError
      storageLink = storageLinks.find(_.localBaseDirectory == req.localObjectPath)
      metadata <- checkMetadata(GetMetadataRequest(req.localObjectPath))
      _ <- if(metadata.syncStatus == SyncStatus.Live) {
        io.file.readAll[IO](Paths.get(req.localObjectPath.asString), blockingEc, 4096).compile.to[Array].flatMap {
          body =>
            googleStorageService.storeObject(GcsBucketName(""), GcsBlobName(""), body, "text/plain", Some(traceId))
        }
      } else {
        logger.info("File is outdated, hence not delocalizing")
      }
    } yield ()
  }
}

object ObjectService {
  def apply(googleStorageService: GoogleStorageService[IO], blockingEc: ExecutionContext, pathToStorageLinks: Path)(implicit cs: ContextShift[IO], logger: Logger[IO]): ObjectService = new ObjectService(googleStorageService, blockingEc, pathToStorageLinks)

  implicit val actionDecoder: Decoder[Action] = Decoder.decodeString.emap {
    str =>
      Action.stringToAction.get(str).toRight("invalid action")
  }

  implicit val entryDecoder: Decoder[Entry] = Decoder.instance {
    cursor =>
      for {
        bucketAndObject <- cursor.downField("sourceUri").as[BucketNameAndObjectName]
        localObjectPath <- cursor.downField("localDestinationPath").as[LocalObjectPath]
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

  implicit val metadataResponseEncoder: Encoder[MetadataResponse] = Encoder.forProduct7(
    "isLinked",
    "syncStatus",
    "lastEditedBy",
    "lastEditedTime",
    "remoteUri",
    "isExecutionMode",
    "storageLink"
  )(x => MetadataResponse.unapply(x).get)
}

final case class GetMetadataRequest(localObjectPath: LocalObjectPath)
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
  final case class SafeDelocalize(localObjectPath: LocalObjectPath) extends PostObjectRequest {
    override def action: Action = Action.SafeDelocalize
  }
}

final case class Entry(bucketNameAndObjectName: BucketNameAndObjectName, localObjectPath: LocalObjectPath)

final case class LocalizeRequest(entries: List[Entry])
final case class MetadataResponse(isLinked: Boolean,
                                  syncStatus: SyncStatus,
                                  lastEditedBy: WorkbenchEmail,
                                  lastEditedTime: Instant,
                                  remoteUri: Uri,
                                  isExecutionMode: Boolean,
                                  storageLinks: StorageLink
                                 )