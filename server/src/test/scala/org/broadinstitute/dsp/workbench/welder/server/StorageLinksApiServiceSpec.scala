package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.{Path, Paths}
import java.util.UUID

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import fs2.{Stream, text}
import io.circe.{Json, parser}
import org.broadinstitute.dsde.workbench.google2.RemoveObjectResult
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.FlatSpec

import scala.util.matching.Regex

class StorageLinksApiServiceSpec extends FlatSpec with WelderTestSuite {
  val storageLinks = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
  val workingDirectory = Paths.get("/tmp")
  val googleStorageAlg = new GoogleStorageAlg {
    override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] = IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
    override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = ???
    override def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] = ???
    override def gcsToLocalFile(localAbsolutePath: Path, gsPath: GsPath, traceId: TraceId): Stream[IO, AdaptedGcsMetadata] = ???
    override def delocalize(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, userDefinedMeta: Map[String, String], traceId: TraceId): IO[DelocalizeResponse] = ???
    override def localizeCloudDirectory(localBaseDirectory: RelativePath, cloudStorageDirectory: CloudStorageDirectory, workingDir: Path, patter: Regex, traceId: TraceId): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
  }
  val metadataCacheAlg = new MetadataCacheInterp(Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty))
  val storageLinksService = StorageLinksService(storageLinks, googleStorageAlg, metadataCacheAlg, StorageLinksServiceConfig(workingDirectory, Paths.get("/tmp/WORKSPACE_BUCKET")))
  val cloudStorageDirectory = CloudStorageDirectory(GcsBucketName("foo"), Some(BlobPath("bar")))
  val baseDir = RelativePath(Paths.get("foo"))
  val baseSafeDir = RelativePath(Paths.get("bar"))


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

    val expectedBody = """{"storageLinks":[{"localBaseDirectory":"foo","localSafeModeBaseDirectory":"bar","cloudStorageDirectory":"gs://foo/bar","pattern":".zip"}]}"""

    val linkToAdd = StorageLink(LocalBaseDirectory(baseDir), LocalSafeBaseDirectory(baseSafeDir), cloudStorageDirectory, ".zip".r)

    storageLinksService.createStorageLink(linkToAdd).run(TraceId(UUID.randomUUID().toString)).unsafeRunSync()

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