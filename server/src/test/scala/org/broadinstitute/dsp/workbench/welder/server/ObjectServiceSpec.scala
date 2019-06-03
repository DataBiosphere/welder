package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.file.{Path, Paths}

import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import io.circe.{Json, parser}
import fs2.{io, text}
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.FlatSpec
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.global

class ObjectServiceSpec extends FlatSpec with WelderTestSuite {
  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]
  val storageLinksCache = Ref.unsafe[IO, Map[LocalBasePath, StorageLink]](Map.empty)
  val metaCache = Ref.unsafe[IO, Map[Path, GcsMetadata]](Map.empty)
  val objectServiceConfig = ObjectServiceConfig(Paths.get("/tmp"), WorkbenchEmail("me@gmail.com"), 20 minutes)
  val objectService = ObjectService(objectServiceConfig, FakeGoogleStorageInterpreter, global, storageLinksCache, metaCache)

  "ObjectService" should "be able to localize a file" in {
    forAll {(bucketName: GcsBucketName, blobName: GcsBlobName, bodyString: String, localFileDestination: Path) => //Use string here just so it's easier to debug
      val body = bodyString.getBytes()
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
        _ <- FakeGoogleStorageInterpreter.storeObject(bucketName, blobName, body, "text/plain", Map.empty, None, None).compile.drain
        resp <- objectService.service.run(request).value
        localFileBody <- io.file.readAll[IO](localFileDestination, global, 4096)
          .compile
          .toList
        _ <- IO((new File(localFileDestination.toString)).delete())
      } yield {
        resp.get.status shouldBe (Status.Ok)
        localFileBody should contain theSameElementsAs (body)
      }

      res.unsafeRunSync()
    }
  }

  it should "should be able to localize data uri" in {
    val localFileDestination = arbFilePath.arbitrary.sample.get
    val requestBody =
      s"""
         |{
         |  "action": "localize",
         |  "entries": [
         |   {
         |     "sourceUri": "data:application/json;base64,eyJkZXN0aW5hdGlvbiI6ICJnczovL2J1Y2tldC9ub3RlYm9va3MiLCAicGF0dGVybiI6ICJcLmlweW5iJCJ9",
         |     "localDestinationPath": "${localFileDestination}"
         |   }
         |  ]
         |}
      """.stripMargin

    val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
    val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)

    val expectedBody = """{"destination": "gs://bucket/notebooks", "pattern": "\.ipynb$"}""".stripMargin

    val res = for {
      resp <- objectService.service.run(request).value
      localFileBody <- io.file.readAll[IO](localFileDestination, global, 4096).through(fs2.text.utf8Decode).compile.foldMonoid
      _ <- IO((new File(localFileDestination.toString)).delete())
    } yield {
      resp.get.status shouldBe (Status.Ok)
      localFileBody shouldBe(expectedBody)
    }

    res.unsafeRunSync()
  }

  "/GET metadata" should "return safe mode if storagelink isn't found" in {
    forAll {
      (bucketName: GcsBucketName, blobName: GcsBlobName, localFileDestination: Path) =>
        val requestBody = s"""
             |{
             |  "localPath": "${localFileDestination.toString}"
             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)

        val res = for {
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
        } yield {
          resp.get.status shouldBe Status.Ok
        }
        res.attempt.unsafeRunSync() shouldBe(Left(StorageLinkNotFoundException(s"No storage link found for ${localFileDestination.toString}")))
    }
  }
}