package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect._
import org.http4s.HttpApp
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.http4s.server.middleware.Logger
import org.http4s.syntax.kleisli._

import scala.util.control.NoStackTrace

class WelderApp(objectService: ObjectService)(implicit cs: ContextShift[IO]) extends Http4sDsl[IO] {
  private val routes: HttpApp[IO] = Router[IO](
    "/status" -> StatusService.service,
    "/storageLinks" -> StorageLinksService.service,
    "/objects" -> objectService.service
  ).orNotFound

  val service: HttpApp[IO] = Logger.httpApp(true, true)(routes)
}

object WelderApp {
  def apply(syncService: ObjectService)(implicit cs: ContextShift[IO]): WelderApp = new WelderApp(syncService)
}

sealed abstract class WelderException extends NoStackTrace {
  def message: String

  override def getMessage: String = message
}
final case class BadRequestException(message: String) extends WelderException
final case class NotFoundException(message: String) extends WelderException