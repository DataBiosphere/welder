package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.file.Paths

import cats.effect.IO
import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Json, parser}
import fs2.io
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.FlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.broadinstitute.dsp.workbench.welder.Generators.{arbGcsBlobName, arbGcsBucketName}

import scala.concurrent.ExecutionContext.global

class ObjectServiceSpec extends FlatSpec with ScalaCheckPropertyChecks with WelderTestSuite {
  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]
  val objectService = ObjectService(FakeGoogleStorageInterpreter, global, Paths.get("fakePath"))

  "ObjectService" should "be able to localize a file" in {
    forAll { (bucketName: GcsBucketName, blobName: GcsBlobName, body: Array[Byte]) =>
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

        responseBody should contain theSameElementsAs (body)
        (new File(localFileDestination)).delete()
      }

      res.unsafeRunSync()
    }
  }
//
//  it should "fail to retrieve metadata with " in {
//    forAll {
//      (bucketName: GcsBucketName, blobName: GcsBlobName, body: Array[Byte]) =>
//        val localFileDestination = "/tmp/localizeTest"
//        // It would be nice to test objects with `/` in its name, but google storage emulator doesn't support it
//        val requestBody = s"""
//             |{
//             |  "localPath": "${localFileDestination}"
//             |}""".stripMargin
//        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
//        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
//
//        val res = for {
//          _ <- FakeGoogleStorageInterpreter.removeObject(bucketName, blobName)
//          _ <- FakeGoogleStorageInterpreter.storeObject(bucketName, blobName, body, "text/plain", None)
//          resp <- objectService.service.run(request).value
//        } yield {
//          resp.get.status shouldBe (Status.Ok)
//          val responseBody = io.file.readAll[IO](Paths.get(localFileDestination), global, 4096)
//            .compile
//            .toList
//            .unsafeRunSync()
//
//          responseBody should contain theSameElementsAs (body)
//        }
//
//        res.unsafeRunSync()
//    }
//  }
}