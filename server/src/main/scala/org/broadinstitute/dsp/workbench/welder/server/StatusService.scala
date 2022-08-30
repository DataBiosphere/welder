package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.IO
import io.circe.Encoder
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.circe.CirceEntityEncoder._

object StatusService extends Http4sDsl[IO] {
  implicit def statusResponseEncoder: Encoder[StatusResponse] =
    Encoder.forProduct2(
      "buildTime",
      "gitHeadCommit"
    )(x => StatusResponse.unapply(x).get)

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] { case GET -> Root =>
    val response = StatusResponse(
      BuildInfo.buildTime.toString,
      BuildInfo.gitHeadCommit
    )

    Ok(response)
  }
}

final case class StatusResponse(buildTime: String, gitHeadCommit: String)
