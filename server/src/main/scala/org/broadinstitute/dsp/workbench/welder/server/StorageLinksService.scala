package org.broadinstitute.dsp.workbench.welder.server

import cats.effect.IO
import cats.effect.concurrent.Ref
import io.circe.{Decoder, Encoder}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import StorageLinksService._

class StorageLinksService(storageLinks: Ref[IO, Map[LocalDirectory, StorageLink]]) extends Http4sDsl[IO] {

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root =>
      Ok(getStorageLinks)
    case req @ DELETE -> Root =>
      for {
        storageLink <- req.as[StorageLink]
        resp <- Ok(deleteStorageLink(storageLink))
      } yield resp
    case req @ POST -> Root =>
      for {
        storageLink <- req.as[StorageLink]
        resp <- Ok(createStorageLink(storageLink))
      } yield resp
  }

  def createStorageLink(storageLink: StorageLink): IO[StorageLink] = {
    storageLinks.modify(links => (links + (storageLink.localBaseDirectory -> storageLink), links)).map(_ => storageLink)
  }

  def deleteStorageLink(storageLink: StorageLink): IO[Unit] = {
    storageLinks.modify(links => (links - storageLink.localBaseDirectory, links)).map(_ => ())
  }

  def getStorageLinks: IO[StorageLinks] = {
    storageLinks.get.map(links => StorageLinks(links.values.toSet))
  }
}

final case class LocalDirectory(asString: String) extends AnyVal
final case class GsDirectory(asString: String) extends AnyVal
final case class StorageLink(localBaseDirectory: LocalDirectory, cloudStorageDirectory: GsDirectory, pattern: String, recursive: Boolean)
final case class StorageLinks(storageLinks: Set[StorageLink])

object StorageLinksService {
  def apply(storageLinks: Ref[IO, Map[LocalDirectory, StorageLink]]): StorageLinksService = new StorageLinksService(storageLinks)

  implicit val localDirectoryEncoder: Encoder[LocalDirectory] = Encoder.encodeString.contramap(_.asString)
  implicit val gsDirectoryEncoder: Encoder[GsDirectory] = Encoder.encodeString.contramap(_.asString)

  implicit val localDirectoryDecoder: Decoder[LocalDirectory] = Decoder.decodeString.map(LocalDirectory)
  implicit val gsDirectoryDecoder: Decoder[GsDirectory] = Decoder.decodeString.map(GsDirectory)

  implicit val storageLinkEncoder: Encoder[StorageLink] = Encoder.forProduct4(
    "localBaseDirectory",
    "cloudStorageDirectory",
    "pattern",
    "recursive")(storageLink => StorageLink.unapply(storageLink).get)

  implicit val storageLinkDecoder: Decoder[StorageLink] = Decoder.forProduct4(
    "localBaseDirectory",
    "cloudStorageDirectory",
    "pattern",
    "recursive")(StorageLink.apply)

  implicit val storageLinksEncoder: Encoder[StorageLinks] = Encoder.forProduct1(
    "storageLinks"
  )(storageLinks => StorageLinks.unapply(storageLinks).get)

  implicit val storageLinksDecoder: Decoder[StorageLinks] = Decoder.forProduct1(
    "storageLinks"
  )(StorageLinks.apply)
}
