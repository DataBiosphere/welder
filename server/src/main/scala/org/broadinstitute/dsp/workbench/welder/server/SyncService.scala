package org.broadinstitute.dsp.workbench.welder.server

import cats.effect.IO
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.circe.CirceEntityDecoder._
import SyncService._

class SyncService(googleStorageService: GoogleStorageService[IO]) extends Http4sDsl[IO] {
  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ POST -> Root / "localize" =>
      for {
        localizeReq <- req.as[LocalizeRequest]
        resp <- Ok(localize(localizeReq))
      } yield resp
    case POST -> Root / "checkMeta" =>
      Ok(checkMeta())
    case POST -> Root / "safeDelocalize" =>
      Ok(safeDelocalize())
  }

  def localize(req: LocalizeRequest): IO[Unit] = ???

  def checkMeta(): IO[Unit] = ???

  def safeDelocalize(): IO[Unit] = ???
}

object SyncService {
  def apply(googleStorageService: GoogleStorageService[IO]): SyncService = new SyncService(googleStorageService)

  implicit val localizeRequestDecoder: Decoder[LocalizeRequest] = Decoder.forProduct1("entries")(LocalizeRequest.apply)
}

final case class LocalizeRequest(entries: List[String]) //TODO: fix this
