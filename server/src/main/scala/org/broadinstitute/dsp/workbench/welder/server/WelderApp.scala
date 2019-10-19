package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect._
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.circe.Encoder
import org.broadinstitute.dsp.workbench.welder.server.WelderApp._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.http4s.server.middleware.{Logger => HttpLogger}
import org.http4s.syntax.kleisli._
import org.http4s.{HttpApp, Response}

class WelderApp(objectService: ObjectService, storageLinksService: StorageLinksService, cacheService: ShutdownService)(
    implicit cs: ContextShift[IO],
    logger: Logger[IO]
) extends Http4sDsl[IO] {
  private val routes: HttpApp[IO] = Router[IO](
    "/status" -> StatusService.service,
    "/storageLinks" -> storageLinksService.service,
    "/objects" -> objectService.service,
    "/cache" -> cacheService.service
  ).orNotFound

  val errorHandler: IO[Response[IO]] => IO[Response[IO]] = response => {
    response.attempt.flatMap { res =>
      res match {
        case Left(error) =>
          val resp = error match {
            case BadRequestException(_, message) => BadRequest(ErrorReport(message))
            case e: org.http4s.InvalidMessageBodyFailure => BadRequest(ErrorReport(e.getCause().toString))
            case NotFoundException(_, message) => NotFound(ErrorReport(message))
            case GenerationMismatch(_, x) => PreconditionFailed(ErrorReport(x, Some(0)))
            case StorageLinkNotFoundException(_, x) => PreconditionFailed(ErrorReport(x, Some(1)))
            case SafeDelocalizeSafeModeFileError(_, x) => PreconditionFailed(ErrorReport(x, Some(2)))
            case DeleteSafeModeFileError(_, x) => PreconditionFailed(ErrorReport(x, Some(3)))
            case InvalidLock(_, x) => PreconditionFailed(ErrorReport(x, Some(4)))
            case InternalException(_, x) => InternalServerError(ErrorReport(x))
            case LockedByOther(_, x) => Conflict(x)
            case e =>
              val errorMessage = if (e.getCause != null) e.getCause.toString else e.toString
              InternalServerError(ErrorReport(errorMessage))
          }
          logger.error(s"Error response: ${error.getMessage}") >> resp
        case Right(value) =>
          IO.pure(value)
      }
    }
  }

  val service: HttpApp[IO] = HttpLogger.httpApp(true, true)(routes).mapF(errorHandler)
}

object WelderApp {
  def apply(syncService: ObjectService, storageLinksService: StorageLinksService, cacheService: ShutdownService)(
      implicit cs: ContextShift[IO],
      logger: Logger[IO]
  ): WelderApp =
    new WelderApp(syncService, storageLinksService, cacheService)

  implicit val errorReportEncoder: Encoder[ErrorReport] = Encoder.forProduct2("errorMessage", "errorCode")(x => ErrorReport.unapply(x).get)
}

final case class ErrorReport(message: String, errorCode: Option[Int] = None)
