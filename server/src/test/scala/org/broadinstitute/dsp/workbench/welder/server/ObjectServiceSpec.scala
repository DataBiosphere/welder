package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.file.Paths

import cats.effect.IO
import io.circe.{Json, parser}
import fs2.{io, text}
import org.broadinstitute.dsde.workbench.google2.Generators._
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.global

class ObjectServiceSpec extends FlatSpec with WelderTestSuite {
  val objectService = ObjectService(FakeGoogleStorageInterpreter, global)
  "ObjectService" should "be able to localize a file" in {
    val bucketName = genGcsBucketName.sample.get
    val blobName = genGcsBlobName.sample.get
    val body = genGcsObjectBody.sample.get
    val localFileDestination = "/tmp/localizeTest"
    // It would be nice to test objects with `/` in its name, but google storage emulator doesn't support it
    val requestBody =
      s"""
        |{
        |  "action": "localize",
        |  "entries": [
        |   {
        |     "sourceUri": "gs://${bucketName.value}/${blobName.value}",
        |     "localDestinationPath": "${localFileDestination}"
        |   }
        |  ]
        |}
      """.stripMargin
    val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
    val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)

    val testBOdy = fs2.Stream.emits(body) through(text.utf8Decode)

    val res = for {
      _ <- FakeGoogleStorageInterpreter.removeObject(bucketName, blobName)
      _ <- FakeGoogleStorageInterpreter.storeObject(bucketName, blobName, body, "text/plain", None)
      resp <- objectService.service.run(request).value
    } yield {
      resp.get.status shouldBe (Status.Ok)
      val responseBody = io.file.readAll[IO](Paths.get(localFileDestination), global, 4096)
        .compile
        .toList
        .unsafeRunSync()
      val re = fs2.Stream.emits(responseBody) through(text.utf8Decode)

      responseBody should contain theSameElementsAs (body)
      (new File(localFileDestination)).delete()
    }

    res.unsafeRunSync()
  }
}