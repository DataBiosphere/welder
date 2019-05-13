package org.broadinstitute.dsp.workbench.welder
package server

import java.time.Instant

import cats.effect.IO
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.http4s.{HttpRoutes, Uri}
import org.http4s.dsl.Http4sDsl
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import ObjectService._
import JsonCodec._
import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsp.workbench.welder.server.PostObjectRequest._

class ObjectService(googleStorageService: GoogleStorageService[IO]) extends Http4sDsl[IO] {
  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ GET -> Root / "metadata" =>
      for {
        metadataReq <- req.as[GetMetadataRequest]
        resp <- Ok(checkMeta(metadataReq))
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

  def localize(req: Localize): IO[Unit] = ???

  def checkMeta(req: GetMetadataRequest): IO[MetadataResponse] = ???

  def safeDelocalize(req: SafeDelocalize): IO[Unit] = ???
}

object ObjectService {
  def apply(googleStorageService: GoogleStorageService[IO]): ObjectService = new ObjectService(googleStorageService)

  implicit val actionDecoder: Decoder[Action] = Decoder.decodeString.emap {
    str =>
      Action.stringToAction.get(str).toRight("invalid action")
  }

  implicit val localizeDecoder: Decoder[Localize] = Decoder.forProduct1("entries"){
    Localize.apply
  }

  implicit val safeDelocalizeDecoder: Decoder[SafeDelocalize] = Decoder.forProduct1("localPath"){
    SafeDelocalize.apply
  }

  implicit val localizeRequestDecoder: Decoder[PostObjectRequest] = Decoder.instance {
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
    "remoteUri",
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
  final case class Localize(entries: List[String]) extends PostObjectRequest {
    override def action: Action = Action.Localize
  }
  final case class SafeDelocalize(localObjectPath: LocalObjectPath) extends PostObjectRequest {
    override def action: Action = Action.SafeDelocalize
  }
}
final case class LocalizeRequest(entries: List[String]) //TODO: fix this

final case class MetadataResponse(
                                   isLinked: Boolean,
                                  syncStatus: SyncStatus,
                                  lastEditedBy: WorkbenchEmail,
                                  lastEditedTime: Instant,
                                  remoteUri: Uri,
                                  storageLinks: String //TODO: fix this
                                 )