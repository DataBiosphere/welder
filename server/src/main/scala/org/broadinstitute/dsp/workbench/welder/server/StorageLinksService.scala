package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.IO
import cats.effect.concurrent.Ref
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.server.StorageLinksService._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl

class StorageLinksService(storageLinks: Ref[IO, Map[LocalDirectory, StorageLink]]) extends Http4sDsl[IO] {

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root =>
      Ok(getStorageLinks)
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
  def createStorageLink(storageLink: StorageLink): IO[StorageLink] = {
    storageLinks.modify {
      links =>
        val toAdd = List(storageLink.localBaseDirectory -> storageLink, storageLink.localSafeModeBaseDirectory -> storageLink).toMap
        (links ++ toAdd, storageLink)
    }
  }

  def deleteStorageLink(storageLink: StorageLink): IO[Unit] = {
    storageLinks.modify {
      links =>
        val toDelete = List(storageLink.localBaseDirectory, storageLink.localSafeModeBaseDirectory)
        (links -- toDelete, ())
    }
  }

  def getStorageLinks: IO[StorageLinks] = {
    storageLinks.get.map(links => StorageLinks(links.values.toSet))
  }
}

final case class StorageLinks(storageLinks: Set[StorageLink])

object StorageLinksService {
  def apply(storageLinks: StorageLinksCache): StorageLinksService = new StorageLinksService(storageLinks)

  implicit val storageLinksEncoder: Encoder[StorageLinks] = Encoder.forProduct1(
    "storageLinks"
  )(storageLinks => StorageLinks.unapply(storageLinks).get)

  implicit val storageLinksDecoder: Decoder[StorageLinks] = Decoder.forProduct1(
    "storageLinks"
  )(StorageLinks.apply)
}
