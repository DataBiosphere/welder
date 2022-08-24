package org.broadinstitute.dsp.workbench.welder
package server

import _root_.io.circe.Decoder
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.implicits._
import cats.mtl.Ask
import fs2.concurrent.SignallingRef
import fs2.io.file.Files
import fs2.{Pipe, Stream}
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.flatspec.AnyFlatSpec
import org.typelevel.jawn.AsyncParser

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
import scala.util.matching.Regex

class ShutdownServiceSpec extends AnyFlatSpec with WelderTestSuite {
  val config =
    PreshutdownServiceConfig(
      CloudStorageBlob("welder-metadata/storagelinks.json"),
      CloudStorageBlob("welder-metadata/metadata.json"),
      Paths.get("/tmp"),
      CloudStorageContainer("fakeStagingBucket")
    )
  val fakeGoogleStorageAlg = Ref.unsafe[IO, CloudStorageAlg](new MockCloudStorageAlg {
    override def updateMetadata(gsPath: SourceUri, metadata: Map[String, String])(implicit ev: Ask[IO, TraceId]): IO[UpdateMetadataResponse] =
      IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
    override def localizeCloudDirectory(
        localBaseDirectory: RelativePath,
        cloudStorageDirectory: CloudStorageDirectory,
        workingDir: Path,
        pattern: Regex
    )(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadataCache]] = Stream.empty
    override def fileToGcs(localObjectPath: RelativePath, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
    override def fileToGcsAbsolutePath(localFile: Path, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
      Files[IO].readAll(fs2.io.file.Path.fromNioPath(localFile)).compile.to(Array).flatMap { body =>
        FakeGoogleStorageInterpreter
          .createBlob(gsPath.asInstanceOf[GsPath].bucketName, gsPath.asInstanceOf[GsPath].blobName, body, gcpObjectType, Map.empty, None)
          .void
          .compile
          .lastOrError
      }
    override def uploadBlob(path: SourceUri)(implicit ev: Ask[IO, TraceId]): Pipe[IO, Byte, Unit] =
      FakeGoogleStorageInterpreter
        .streamUploadBlob(path.asInstanceOf[GsPath].bucketName, path.asInstanceOf[GsPath].blobName)

    override def getBlob[A: Decoder](path: SourceUri)(implicit ev: Ask[IO, TraceId]): Stream[IO, A] = path match {
      case GsPath(bucketName, blobName) =>
      for {
        blob <- FakeGoogleStorageInterpreter.getBlob(bucketName, blobName, None)
        a <- Stream
          .emits(blob.getContent())
          .through(fs2.text.utf8.decode)
          .through(_root_.io.circe.fs2.stringParser[IO](AsyncParser.SingleValue))
          .through(_root_.io.circe.fs2.decoder[IO, A])
      } yield a
      case _ => Stream.eval(IO.raiseError(new RuntimeException("test error, wrong source URI given to getBlob in a MockCloudStorageAlg")))
    }
  })

  "CacheService" should "return flush cache and log files" in {
    forAll { (localPath: RelativePath, storageLink: StorageLink) =>
      val metadata = AdaptedGcsMetadataCache(localPath, RemoteState.Found(None, Crc32("sfds")), None)
      val signal = SignallingRef[IO, Boolean](false).unsafeRunSync()
      val cacheService = ShutdownService(
        config,
        signal,
        Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localPath -> storageLink)),
        Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map(localPath -> metadata)),
        fakeGoogleStorageAlg
      )
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/flush"))

      val welderLogContent = "this is welder!"
      val jupyterLogContent = "this is jupyter!"

      val res = for {
        storageAlg <- fakeGoogleStorageAlg.get
        // write two fake log files
        _ <- Stream.emits(welderLogContent.getBytes("UTF-8")).through(Files[IO].writeAll(fs2.io.file.Path("/tmp/.welder.log"))).compile.drain
        _ <- Stream.emits(jupyterLogContent.getBytes("UTF-8")).through(Files[IO].writeAll(fs2.io.file.Path("/tmp/jupyter.log"))).compile.drain

        resp <- cacheService.service.run(request).value
        gcsMetadata <- storageAlg
          .getBlob[List[AdaptedGcsMetadataCache]](GsPath(config.stagingBucketName.asGcsBucket, config.gcsMetadataJsonBlobName.asGcs))
          .compile
          .lastOrError
        storageLinksCache <- storageAlg
          .getBlob[List[StorageLink]](GsPath(config.stagingBucketName.asGcsBucket, config.storageLinksJsonBlobName.asGcs))
          .compile
          .lastOrError
        welderLogInGcs <- FakeGoogleStorageInterpreter
          .getBlob(config.stagingBucketName.asGcsBucket, GcsBlobName(s"cluster-log-files/.welder.log"))
          .compile
          .lastOrError
        jupyterLogInGcs <- FakeGoogleStorageInterpreter
          .getBlob(config.stagingBucketName.asGcsBucket, GcsBlobName(s"cluster-log-files/jupyter.log"))
          .compile
          .lastOrError
        _ <- IO((new File("/tmp/.welder.log")).delete())
        _ <- IO((new File("/tmp/jupyter.log")).delete())
      } yield {
        resp.get.status shouldBe Status.NoContent

        gcsMetadata shouldBe List(metadata)
        val sl = storageLinksCache(0) //default equality check doesn't work for StorageLink
        sl.localBaseDirectory shouldBe storageLink.localBaseDirectory
        sl.localSafeModeBaseDirectory shouldBe storageLink.localSafeModeBaseDirectory
        sl.cloudStorageDirectory shouldBe storageLink.cloudStorageDirectory
        sl.pattern.toString() shouldBe storageLink.pattern.toString
        new String(welderLogInGcs.getContent(), StandardCharsets.UTF_8) shouldBe welderLogContent
        new String(jupyterLogInGcs.getContent(), StandardCharsets.UTF_8) shouldBe jupyterLogContent
      }
      res.unsafeRunSync()
    }
  }
}
