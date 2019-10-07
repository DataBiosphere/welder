package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.file.{Path, Paths}

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.mtl.ApplicativeAsk
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.broadinstitute.dsde.workbench.google2.{Crc32, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.matching.Regex

class ShutdownServiceSpec extends FlatSpec with Matchers with WelderTestSuite {
  val config = PreshutdownServiceConfig(Paths.get("/tmp/storagelinks.json"), Paths.get("/tmp/metadata.json"), RelativePath(Paths.get("welder.log")), GcsBucketName("fakeStagingBucket"))
  val fakeGoogleStorageAlg = new GoogleStorageAlg {
    override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] = IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
    override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = ???
    override def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] = ???
    override def gcsToLocalFile(localAbsolutePath: Path, gsPath: GsPath, traceId: TraceId): Stream[IO, AdaptedGcsMetadata] = ???
    override def delocalize(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, userDefinedMeta: Map[String, String], traceId: TraceId): IO[DelocalizeResponse] = ???
    override def localizeCloudDirectory(localBaseDirectory: RelativePath, cloudStorageDirectory: CloudStorageDirectory, workingDir: Path, pattern: Regex, traceId: TraceId): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
    override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit
  }

  "CacheService" should "return flush cache" in {
    forAll {
      (localPath: RelativePath, storageLink: StorageLink) =>
        val metadata = AdaptedGcsMetadataCache(localPath, RemoteState.Found(None, Crc32("sfds")), None)
        val signal = SignallingRef[IO, Boolean](false).unsafeRunSync()
        val cacheService = ShutdownService (
          config,
          signal,
          Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localPath -> storageLink)),
          Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map(localPath -> metadata)),
          fakeGoogleStorageAlg,
          blocker
        )
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/flush"))

        val res = for {
          resp <- cacheService.service.run(request).value
          storageLinkResult <- readJsonFileToA[IO, List[StorageLink]](config.storageLinksPath, blocker).compile.lastOrError
          metadataResult <- readJsonFileToA[IO, List[AdaptedGcsMetadataCache]](config.metadataCachePath, blocker).compile.lastOrError
          _ <- IO((new File(config.storageLinksPath.toString)).delete())
          _ <- IO((new File(config.metadataCachePath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.NoContent

          assert(storageLinkEq.eqv(storageLinkResult(0), (storageLink)))
          metadataResult shouldBe(List(metadata))
        }
      res.unsafeRunSync()
    }
  }
}


