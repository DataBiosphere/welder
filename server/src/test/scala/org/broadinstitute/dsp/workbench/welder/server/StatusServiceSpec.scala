package org.broadinstitute.dsp.workbench.welder
package server

import io.circe.Decoder
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.{FlatSpec, Matchers}
import org.http4s.circe.CirceEntityDecoder._
import StatusServiceSpec._
import cats.effect.IO

class StatusServiceSpec extends FlatSpec with Matchers {
  "StatusService" should "return service status" in {
    val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/"))
    val resp = StatusService.service.run(request).value.unsafeRunSync().get
    resp.status shouldBe(Status.Ok)

    val expectedResp = StatusResponse(BuildInfo.buildTime.toString, BuildInfo.gitHeadCommit)
    resp.as[StatusResponse].unsafeRunSync() shouldBe (expectedResp)
  }
}

object StatusServiceSpec {
  implicit def statusResponseEncoder: Decoder[StatusResponse] = Decoder.forProduct2(
    "buildTime",
    "gitHeadCommit"
  )(StatusResponse.apply)
}