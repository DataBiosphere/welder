package org.broadinstitute.dsp.workbench.welder.server

import java.util.UUID

import cats.effect.IO
import org.broadinstitute.dsde.workbench.model.TraceId
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Request, Response}

trait WelderService extends Http4sDsl[IO] {
  private def extractTraceId(req: Request[IO]): IO[TraceId] = {
    val traceIdFromHeader = req.headers.get(org.typelevel.ci.CIString("X-Cloud-Trace-Context")).map(x => x.map(h => TraceId(h.value)))
    traceIdFromHeader.fold(IO(TraceId(UUID.randomUUID().toString)))(x => IO.pure(x.head))
  }

  def withTraceId(route: PartialFunction[Request[IO], (TraceId => IO[Response[IO]])]): HttpRoutes[IO] = HttpRoutes.of {
    case req if route.isDefinedAt(req) =>
      for {
        traceId <- extractTraceId(req)
        resp <- route(req)(traceId)
      } yield resp
  }
}
