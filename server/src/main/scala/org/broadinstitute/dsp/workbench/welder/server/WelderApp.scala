package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect._
import cats.implicits._
import org.typelevel.log4cats.StructuredLogger
import io.circe.Encoder
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.workbench.welder.server.WelderApp._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.http4s.server.middleware.{Logger => HttpLogger}
import org.http4s.syntax.kleisli._
import org.http4s.{HttpApp, Response}

class WelderApp(objectService: ObjectService, storageLinksService: StorageLinksService, cacheService: ShutdownService)(
    implicit cs: ContextShift[IO],
    logger: StructuredLogger[IO]
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
          error match {
            case e: org.http4s.InvalidMessageBodyFailure =>
              logger.error(s"Invalid json body: ${e.getMessage}") >> BadRequest(ErrorReport(e.getCause().toString, None, None))
            case e: WelderException =>
              val resp = e match {
                case BadRequestException(traceId, message, _) => BadRequest(ErrorReport(message, None, Some(traceId)))
                case NotFoundException(traceId, message, _) => NotFound(ErrorReport(message, None, Some(traceId)))
                case GenerationMismatch(traceId, x, _) => PreconditionFailed(ErrorReport(x, Some(0), Some(traceId)))
                case StorageLinkNotFoundException(traceId, x, _) => PreconditionFailed(ErrorReport(x, Some(1), Some(traceId)))
                case SafeDelocalizeSafeModeFileError(traceId, x, _) => PreconditionFailed(ErrorReport(x, Some(2), Some(traceId)))
                case DeleteSafeModeFileError(traceId, x, _) => PreconditionFailed(ErrorReport(x, Some(3), Some(traceId)))
                case InvalidLock(traceId, x, _) => PreconditionFailed(ErrorReport(x, Some(4), Some(traceId)))
                case InternalException(traceId, x, _) => InternalServerError(ErrorReport(x, None, Some(traceId)))
                case LockedByOther(traceId, x, _) => Conflict(ErrorReport(x, None, Some(traceId)))
              }
              logger.error(e.ctx)(s"Error response: ${e.getMessage}") >> resp
            case e =>
              val errorMessage = if (e.getCause != null) e.getCause.toString else e.toString
              logger.error(e)(s"Unknown error: ${e.getMessage}") >> InternalServerError(ErrorReport(errorMessage, None, None))
          }
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
      logger: StructuredLogger[IO]
  ): WelderApp =
    new WelderApp(syncService, storageLinksService, cacheService)

  implicit val traceIdEncoder: Encoder[TraceId] = Encoder.encodeString.contramap(_.asString)
  implicit val errorReportEncoder: Encoder[ErrorReport] = Encoder.forProduct3("errorMessage", "errorCode", "traceId")(x => ErrorReport.unapply(x).get)
}

final case class ErrorReport(message: String, errorCode: Option[Int] = None, traceId: Option[TraceId])
