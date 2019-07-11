package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect._
import io.circe.Encoder
import org.broadinstitute.dsp.workbench.welder.server.WelderApp._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.http4s.server.middleware.Logger
import org.http4s.syntax.kleisli._
import org.http4s.{HttpApp, Response}

class WelderApp(objectService: ObjectService, storageLinksService: StorageLinksService, cacheService: CacheService)(implicit cs: ContextShift[IO])
    extends Http4sDsl[IO] {
  private val routes: HttpApp[IO] = Router[IO](
    "/status" -> StatusService.service,
    "/storageLinks" -> storageLinksService.service,
    "/objects" -> objectService.service,
    "/cache" -> cacheService.service
  ).orNotFound

  val errorHandler: IO[Response[IO]] => IO[Response[IO]] = response => {
    response.handleErrorWith {
      case BadRequestException(message) => BadRequest(ErrorReport(message))
      case e: org.http4s.InvalidMessageBodyFailure => BadRequest(ErrorReport(e.getCause().toString))
      case NotFoundException(message) => NotFound(ErrorReport(message))
      case GenerationMismatch(x) => PreconditionFailed(ErrorReport(x, Some(0)))
      case StorageLinkNotFoundException(x) => PreconditionFailed(ErrorReport(x, Some(1)))
      case SafeDelocalizeSafeModeFileError(x) => PreconditionFailed(ErrorReport(x, Some(2)))
      case DeleteSafeModeFileError(x) => PreconditionFailed(ErrorReport(x, Some(3)))
      case InvalidLock(x) => PreconditionFailed(ErrorReport(x, Some(4)))
      case InternalException(x) => InternalServerError(ErrorReport(x))
      case LockedByOther(x) => Conflict(x)
      case e =>
        val errorMessage = if (e.getCause != null) e.getCause.toString else e.toString
        InternalServerError(ErrorReport(errorMessage))
    }
  }

  val service: HttpApp[IO] = Logger.httpApp(true, true)(routes).mapF(errorHandler)
}

object WelderApp {
  def apply(syncService: ObjectService, storageLinksService: StorageLinksService, cacheService: CacheService)(implicit cs: ContextShift[IO]): WelderApp =
    new WelderApp(syncService, storageLinksService, cacheService)

  implicit val errorReportEncoder: Encoder[ErrorReport] = Encoder.forProduct2("errorMessage", "errorCode")(x => ErrorReport.unapply(x).get)
}

final case class ErrorReport(message: String, errorCode: Option[Int] = None)
