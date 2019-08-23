package org.broadinstitute.dsp.workbench.welder.server

import java.util.UUID

import cats.effect.IO
import org.broadinstitute.dsde.workbench.model.TraceId
import org.http4s.dsl.Http4sDsl
import org.http4s.util.CaseInsensitiveString
import org.http4s.{HttpRoutes, Request, Response}

trait WelderService extends Http4sDsl[IO] {
  private def extractTraceId(req: Request[IO]): IO[TraceId] = {
    val traceIdFromHeader = req.headers.get(CaseInsensitiveString("X-Cloud-Trace-Context")).map(x => TraceId(x.value))
    traceIdFromHeader.fold(IO(TraceId(UUID.randomUUID().toString)))(IO.pure)
  }

  def withTraceId(route: PartialFunction[
                             Request[IO],
                             (TraceId => IO[Response[IO]])]): HttpRoutes[IO] = HttpRoutes.of {
    case req if route.isDefinedAt(req) =>
      for {
        traceId <- extractTraceId(req)
        resp <- route(req)(traceId)
      } yield resp
  }
}
