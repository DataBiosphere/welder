package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect._
import io.circe.Encoder
import org.http4s.{HttpApp, Response}
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.http4s.server.middleware.Logger
import org.http4s.syntax.kleisli._
import org.http4s.circe.CirceEntityEncoder._
import WelderApp._

import scala.util.control.NoStackTrace

class WelderApp(objectService: ObjectService, storageLinksService: StorageLinksService)(implicit cs: ContextShift[IO]) extends Http4sDsl[IO] {
  private val routes: HttpApp[IO] = Router[IO](
    "/status" -> StatusService.service,
    "/storageLinks" -> storageLinksService.service,
    "/objects" -> objectService.service
  ).orNotFound

  val errorHandler: IO[Response[IO]] => IO[Response[IO]] = response => {
    response.handleErrorWith{
      case BadRequestException(message) => BadRequest(ErrorReport(message))
      case NotFoundException(message) => NotFound(ErrorReport(message))
      case GenerationMismatch(x) => PreconditionFailed(ErrorReport(x, Some(0)))
      case StorageLinkNotFoundException(x) => PreconditionFailed(ErrorReport(x, Some(1)))
      case SafeDelocalizeSafeModeFile(x) => PreconditionFailed(ErrorReport(x, Some(2)))
      case e => InternalServerError(ErrorReport(e.getMessage))
    }
  }

  val service: HttpApp[IO] = Logger.httpApp(true, true)(routes).mapF(errorHandler)
}

object WelderApp {
  def apply(syncService: ObjectService, storageLinksService: StorageLinksService)(implicit cs: ContextShift[IO]): WelderApp = new WelderApp(syncService, storageLinksService)

  implicit val errorReportEncoder: Encoder[ErrorReport] = Encoder.forProduct2("errorMessage", "errorCode")(x => ErrorReport.unapply(x).get)
}

sealed abstract class WelderException extends NoStackTrace {
  def message: String
  override def getMessage: String = message
}
final case class InternalException(message: String) extends WelderException
final case class BadRequestException(message: String) extends WelderException
final case class GenerationMismatch(message: String) extends WelderException
final case class StorageLinkNotFoundException(message: String) extends WelderException
final case class SafeDelocalizeSafeModeFile(message: String) extends WelderException
final case class NotFoundException(message: String) extends WelderException

final case class ErrorReport(message: String, errorCode: Option[Int] = None)