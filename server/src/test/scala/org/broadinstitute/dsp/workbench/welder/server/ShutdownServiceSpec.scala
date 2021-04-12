package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}

import _root_.io.circe.Decoder
import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.mtl.Ask
import fs2.concurrent.SignallingRef
import fs2.{Pipe, Stream, io}
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.flatspec.AnyFlatSpec
import org.typelevel.jawn.AsyncParser

import scala.util.matching.Regex

class ShutdownServiceSpec extends AnyFlatSpec with WelderTestSuite {
  val config =
    PreshutdownServiceConfig(
      GcsBlobName("welder-metadata/storagelinks.json"),
      GcsBlobName("welder-metadata/metadata.json"),
      Paths.get("/tmp"),
      GcsBucketName("fakeStagingBucket")
    )
  val fakeGoogleStorageAlg = new MockGoogleStorageAlg {
    override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
      IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
    override def localizeCloudDirectory(
        localBaseDirectory: RelativePath,
        cloudStorageDirectory: CloudStorageDirectory,
        workingDir: Path,
        pattern: Regex,
        traceId: TraceId
    ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
    override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
    override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
      io.file.readAll[IO](localFile, blocker, 4096).compile.to(Array).flatMap { body =>
        FakeGoogleStorageInterpreter
          .createBlob(gsPath.bucketName, gsPath.blobName, body, gcpObjectType, Map.empty, None)
          .void
          .compile
          .lastOrError
      }
    override def uploadBlob(bucketName: GcsBucketName, blobName: GcsBlobName): Pipe[IO, Byte, Unit] =
      FakeGoogleStorageInterpreter
        .streamUploadBlob(bucketName, blobName)

    override def getBlob[A: Decoder](bucketName: GcsBucketName, blobName: GcsBlobName): Stream[IO, A] =
      for {
        blob <- FakeGoogleStorageInterpreter.getBlob(bucketName, blobName, None)
        a <- Stream
          .emits(blob.getContent())
          .through(fs2.text.utf8Decode)
          .through(_root_.io.circe.fs2.stringParser[IO](AsyncParser.SingleValue))
          .through(_root_.io.circe.fs2.decoder[IO, A])
      } yield a
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
        gcsMetadata <- fakeGoogleStorageAlg.getBlob[List[AdaptedGcsMetadataCache]](config.stagingBucketName, config.gcsMetadataJsonBlobName).compile.lastOrError
        storageLinksCache <- fakeGoogleStorageAlg.getBlob[List[StorageLink]](config.stagingBucketName, config.storageLinksJsonBlobName).compile.lastOrError
        welderLogInGcs <- FakeGoogleStorageInterpreter.getBlob(config.stagingBucketName, GcsBlobName(s"cluster-log-files/welder.log")).compile.lastOrError
        jupyterLogInGcs <- FakeGoogleStorageInterpreter.getBlob(config.stagingBucketName, GcsBlobName(s"cluster-log-files/jupyter.log")).compile.lastOrError
        _ <- IO((new File("/tmp/welder.log")).delete())
        _ <- IO((new File("/tmp/jupyter.log")).delete())
      } yield {
        resp.get.status shouldBe Status.NoContent

        gcsMetadata shouldBe List(metadata)
        val sl = storageLinksCache(0) //default equality check doesn't work for StorageLink
        sl.localBaseDirectory shouldBe storageLink.localBaseDirectory
        sl.localSafeModeBaseDirectory shouldBe storageLink.localSafeModeBaseDirectory
        sl.cloudStorageDirectory shouldBe storageLink.cloudStorageDirectory
        sl.pattern.toString() shouldBe storageLink.pattern.toString
        new String(welderLogInGcs.getContent(), StandardCharsets.UTF_8) shouldBe welderLogContenct
        new String(jupyterLogInGcs.getContent(), StandardCharsets.UTF_8) shouldBe jupyterLogContenct
      }
      res.unsafeRunSync()
    }
  }
}
