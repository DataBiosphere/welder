package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.{Path, Paths}

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import fs2.text
import io.circe.{Json, parser}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.FlatSpec

class StorageLinksApiServiceSpec extends FlatSpec with WelderTestSuite {
  val storageLinks = Ref.unsafe[IO, Map[Path, StorageLink]](Map.empty)
  val storageLinksService = StorageLinksService(storageLinks)
  val cloudStorageDirectory = CloudStorageDirectory(GcsBucketName("foo"), BlobPath("bar"))
  val baseDir = Paths.get("/foo")
  val baseSafeDir = Paths.get("/bar")

  "GET /storageLinks" should "return 200 and an empty list of no storage links exist" in {
    val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/"))

    val expectedBody = """{"storageLinks":[]}""".stripMargin

    val res = for {
      resp <- storageLinksService.service.run(request).value
      body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
    } yield {
      resp.get.status shouldBe Status.Ok
      body shouldBe expectedBody
    }

    res.unsafeRunSync()
  }

  it should "return 200 and a list of storage links when they exist" in {
    val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/"))

    val expectedBody = """{"storageLinks":[{"localBaseDirectory":"/foo","localSafeModeBaseDirectory":"/bar","cloudStorageDirectory":"gs://foo/bar","pattern":".zip"}]}"""

    val linkToAdd = StorageLink(LocalBaseDirectory(baseDir), LocalSafeBaseDirectory(baseSafeDir), cloudStorageDirectory, ".zip")

    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    val intermediateListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(intermediateListResult.storageLinks equals Set(linkToAdd))

    val res = for {
      resp <- storageLinksService.service.run(request).value
      body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
    } yield {
      resp.get.status shouldBe Status.Ok
      body shouldBe expectedBody
    }

    res.unsafeRunSync()
  }

  "POST /storageLinks" should "return 200 and the storage link created when called with a valid storage link" in {
    val requestBody = """{"localBaseDirectory":"/foo","localSafeModeBaseDirectory":"/bar","cloudStorageDirectory":"gs://foo/bar","pattern":".zip"}"""

    val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
    val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)

    val res = for {
      resp <- storageLinksService.service.run(request).value
      body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
    } yield {
      resp.get.status shouldBe Status.Ok
      body shouldBe requestBody
    }

    res.unsafeRunSync()
  }
}