package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.file.Paths

import cats.effect.IO
import cats.effect.concurrent.Ref
import org.broadinstitute.dsde.workbench.google2.Crc32
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.global

class CacheServiceSpec extends FlatSpec with Matchers with WelderTestSuite {
  val config = CachedServiceConfig(Paths.get("/tmp/storagelinks.json"), Paths.get("/tmp/metadata.json"))


  "CacheService" should "return service status" in {
    forAll {
      (localPath: RelativePath, storageLink: StorageLink) =>
        val metadata = AdaptedGcsMetadataCache(localPath, RemoteState(None, Crc32("sfds")), None)
        val cacheService = CacheService (
          config,
          Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localPath -> storageLink)),
          Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map(localPath -> metadata)),
          global
        )
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/flush"))

        val res = for {
          resp <- cacheService.service.run(request).value
          storageLinkResult <- readJsonFileToA[IO, List[StorageLink]](config.storageLinksPath).compile.lastOrError
          metadataResult <- readJsonFileToA[IO, List[AdaptedGcsMetadataCache]](config.metadataCachePath).compile.lastOrError
          _ <- IO((new File(config.storageLinksPath.toString)).delete())
          _ <- IO((new File(config.metadataCachePath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.NoContent
          storageLinkResult shouldBe(List(storageLink))
          metadataResult shouldBe(List(metadata))
        }
      res.unsafeRunSync()
    }
  }
}


