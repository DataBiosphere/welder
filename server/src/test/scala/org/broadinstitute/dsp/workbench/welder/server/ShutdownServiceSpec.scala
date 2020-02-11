package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}

import cats.implicits._
import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.mtl.ApplicativeAsk
import fs2.{Stream, io}
import fs2.concurrent.SignallingRef
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.util2
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.matching.Regex

class ShutdownServiceSpec extends FlatSpec with Matchers with WelderTestSuite {
  val config =
    PreshutdownServiceConfig(Paths.get("/tmp/storagelinks.json"), Paths.get("/tmp/metadata.json"), Paths.get("/tmp"), GcsBucketName("fakeStagingBucket"))
  val fakeGoogleStorageAlg = new GoogleStorageAlg {
    override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
      IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
    override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = ???
    override def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] = ???
    override def gcsToLocalFile(localAbsolutePath: Path, gsPath: GsPath, traceId: TraceId): Stream[IO, AdaptedGcsMetadata] = ???
    override def delocalize(
        localObjectPath: RelativePath,
        gsPath: GsPath,
        generation: Long,
        userDefinedMeta: Map[String, String],
        traceId: TraceId
    ): IO[DelocalizeResponse] = ???
    override def localizeCloudDirectory(
        localBaseDirectory: RelativePath,
        cloudStorageDirectory: CloudStorageDirectory,
        workingDir: Path,
        pattern: Regex,
        traceId: TraceId
    ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
    override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit
    override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
      io.file.readAll[IO](localFile, blocker, 4096).compile.to(Array).flatMap { body =>
        FakeGoogleStorageInterpreter
          .createBlob(gsPath.bucketName, gsPath.blobName, body, gcpObjectType, Map.empty, None)
          .void
          .compile
          .lastOrError
      }
  }

  "CacheService" should "return flush cache and log files" in {
    forAll { (localPath: RelativePath, storageLink: StorageLink) =>
      val metadata = AdaptedGcsMetadataCache(localPath, RemoteState.Found(None, Crc32("sfds")), None)
      val signal = SignallingRef[IO, Boolean](false).unsafeRunSync()
      val cacheService = ShutdownService(
        config,
        signal,
        Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localPath -> storageLink)),
        Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map(localPath -> metadata)),
        fakeGoogleStorageAlg,
        blocker
      )
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/flush"))

      val welderLogContenct = "this is welder!"
      val jupyterLogContenct = "this is jupyter!"

      val res = for {
        // write two fake log files
        _ <- Stream.emits(welderLogContenct.getBytes("UTF-8")).through(fs2.io.file.writeAll[IO](Paths.get("/tmp/welder.log"), blocker)).compile.drain
        _ <- Stream.emits(jupyterLogContenct.getBytes("UTF-8")).through(fs2.io.file.writeAll[IO](Paths.get("/tmp/jupyter.log"), blocker)).compile.drain

        resp <- cacheService.service.run(request).value
        storageLinkResult <- util2.readJsonFileToA[IO, List[StorageLink]](config.storageLinksPath, Some(blocker)).compile.lastOrError
        metadataResult <- util2.readJsonFileToA[IO, List[AdaptedGcsMetadataCache]](config.metadataCachePath, Some(blocker)).compile.lastOrError
        welderLogInGcs <- FakeGoogleStorageInterpreter.getBlob(config.stagingBucketName, GcsBlobName(s"cluster-log-files/welder.log")).compile.lastOrError
        jupyterLogInGcs <- FakeGoogleStorageInterpreter.getBlob(config.stagingBucketName, GcsBlobName(s"cluster-log-files/jupyter.log")).compile.lastOrError
        _ <- IO((new File(config.storageLinksPath.toString)).delete())
        _ <- IO((new File(config.metadataCachePath.toString)).delete())
        _ <- IO((new File("/tmp/welder.log")).delete())
        _ <- IO((new File("/tmp/jupyter.log")).delete())
      } yield {
        resp.get.status shouldBe Status.NoContent

        new String(welderLogInGcs.getContent(), StandardCharsets.UTF_8) shouldBe welderLogContenct
        new String(jupyterLogInGcs.getContent(), StandardCharsets.UTF_8) shouldBe jupyterLogContenct
        assert(storageLinkEq.eqv(storageLinkResult(0), (storageLink)))
        metadataResult shouldBe (List(metadata))
      }
      res.unsafeRunSync()
    }
  }
}
