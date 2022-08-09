package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonError
import com.google.cloud.storage.Storage
import fs2.io.file.Files
import fs2.{Pipe, Stream}
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google2.Generators.{genGcsBlobName, genGcsObjectBody}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageInterpreterSpec.objectType
import org.broadinstitute.dsde.workbench.google2.mock.{BaseFakeGoogleStorage, FakeGoogleStorageInterpreter}
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.Paths

class GoogleStorageInterpSpec extends AnyFlatSpec with WelderTestSuite {
  //if one day java emulator supports metadata, we shouldn't ignore this test
  ignore should "be able to set metadata when 403 happens" in {
    forAll { (gsPath: GsPath) =>
      val bodyBytes = "this is great!".getBytes("UTF-8")
      val googleStorage = new BaseFakeGoogleStorage {
        override def setObjectMetadata(
            bucketName: GcsBucketName,
            objectName: GcsBlobName,
            metadata: Map[String, String],
            traceId: Option[TraceId],
            retryConfig: RetryConfig,
            blobTargetOptions: List[Storage.BlobTargetOption]
        ): Stream[IO, Unit] = {
          val errors = new GoogleJsonError()
          errors.setCode(403)
          Stream.raiseError[IO](new com.google.cloud.storage.StorageException(errors))
        }
      }
      val googleStorageAlg = CloudStorageAlg.forGoogle(StorageAlgConfig(Paths.get("/tmp")), googleStorage)
      val res = for {
        _ <- googleStorage.createBlob(gsPath.bucketName, gsPath.blobName, bodyBytes, "text/plain", Map.empty, None, None).compile.drain
        _ <- googleStorageAlg.updateMetadata(gsPath, Map("lastLockedBy" -> "me"))
        meta <- googleStorage.getObjectMetadata(gsPath.bucketName, gsPath.blobName, None).compile.lastOrError
      } yield {
        meta.asInstanceOf[GetMetadataResponse.Metadata].userDefined.get("lastLockedBy") shouldBe ("me")
      }
      res.unsafeRunSync()
    }
  }

  "delocalize" should "fail with GenerationMismatch exception if remote file has changed" in {
    forAll { (localObjectPath: RelativePath, gsPath: GsPath) =>
      val bodyBytes = "this is great!".getBytes("UTF-8")
      val googleStorage = CloudStorageAlg.forGoogle(StorageAlgConfig(Paths.get("/tmp")), GoogleStorageServiceWithFailures)
      val localAbsolutePath = Paths.get(s"/tmp/${localObjectPath.asPath.toString}")
      // Create the local base directory
      val directory = new File(s"${localAbsolutePath.getParent.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        _ <- Stream.emits(bodyBytes).covary[IO].through(Files[IO].writeAll(fs2.io.file.Path.fromNioPath(localAbsolutePath))).compile.drain //write to local file
        resp <- googleStorage.delocalize(localObjectPath, gsPath, 0L, Map.empty).attempt
        _ <- IO((new File(localAbsolutePath.toString)).delete())
      } yield {
        resp shouldBe Left(
          GenerationMismatch(fakeTraceId, s"Remote version has changed for ${localAbsolutePath}. Generation mismatch (local generation: 0). null")
        )
      }
      res.unsafeRunSync()
    }
  }

  "gcsToLocalFile" should "be able to download a file from gcs and write to local path" in {
    forAll { (localObjectPath: RelativePath, gsPath: GsPath) =>
      val bodyBytes = "this is great!".getBytes("UTF-8")
      val googleStorage = CloudStorageAlg.forGoogle(StorageAlgConfig(Paths.get("/tmp")), FakeGoogleStorageInterpreter)
      val localAbsolutePath = Paths.get(s"/tmp/${localObjectPath.asPath.toString}")
      // Create the local base directory
      val directory = new File(s"${localAbsolutePath.getParent.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        _ <- FakeGoogleStorageInterpreter.createBlob(gsPath.bucketName, gsPath.blobName, bodyBytes, "text/plain", Map.empty, None)
        resp <- googleStorage.gcsToLocalFile(localAbsolutePath, gsPath)
        _ <- Stream.eval(IO((new File(localAbsolutePath.toString)).delete()))
      } yield {
        val expectedCrc32c = Crc32c.calculateCrc32c(bodyBytes)
        resp shouldBe AdaptedGcsMetadata(None, expectedCrc32c, 0L)
      }
      res.compile.drain.unsafeRunSync()
    }
  }

  it should "overwrite a file if it already exists " in {
    forAll { (localObjectPath: RelativePath, gsPath: GsPath) =>
      val bodyBytes = "this is great!".getBytes("UTF-8")
      val googleStorage = CloudStorageAlg.forGoogle(StorageAlgConfig(Paths.get("/tmp")), FakeGoogleStorageInterpreter)
      val localAbsolutePath = Paths.get(s"/tmp/${localObjectPath.asPath.toString}")
      // Create the local base directory
      val directory = new File(s"${localAbsolutePath.getParent.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        _ <- FakeGoogleStorageInterpreter.createBlob(gsPath.bucketName, gsPath.blobName, bodyBytes, "text/plain", Map.empty, None)
        _ <- (Stream.emits("oldContent".getBytes("UTF-8")).covary[IO] through Files[IO].writeAll(fs2.io.file.Path.fromNioPath(localAbsolutePath))) ++ Stream
          .eval(IO.unit)
        resp <- googleStorage.gcsToLocalFile(localAbsolutePath, gsPath)
        newFileContent <- Files[IO].readAll(fs2.io.file.Path.fromNioPath(localAbsolutePath)).map(x => List(x)).foldMonoid
        _ <- Stream.eval(IO((new File(localAbsolutePath.toString)).delete()))
      } yield {
        val expectedCrc32c = Crc32c.calculateCrc32c(bodyBytes)
        resp shouldBe AdaptedGcsMetadata(None, expectedCrc32c, 0L)
        newFileContent should contain theSameElementsAs (bodyBytes)
      }
      res.compile.drain.unsafeRunSync()
    }
  }

  "localizeCloudDirectory" should "recursively download files for a given CloudStorageDirectory" in {
    forAll { (cloudStorageDirectory: CloudStorageDirectory) =>
      val allObjects = Gen.listOfN(4, genGcsBlobName).sample.get.map { x =>
        cloudStorageDirectory.blobPath match {
          case Some(bp) => GcsBlobName(s"${bp.asString}/${x.value}")
          case None => GcsBlobName(s"${x.value}")
        }
      }
      val objectBody = genGcsObjectBody.sample.get
      val googleStorage = CloudStorageAlg.forGoogle(StorageAlgConfig(Paths.get("/tmp")), FakeGoogleStorageInterpreter)
      val workingDir = Paths.get("/tmp")
      val localBaseDir = RelativePath(Paths.get("edit"))

      val res = for {
        _ <- allObjects.traverse(obj => FakeGoogleStorageInterpreter.createBlob(cloudStorageDirectory.container.asGcsBucket, obj, objectBody, objectType).compile.drain)
        _ <- googleStorage.localizeCloudDirectory(localBaseDir, cloudStorageDirectory, workingDir, "".r).compile.drain
      } yield {
        val prefix = (workingDir.resolve(localBaseDir.asPath))
        val allFiles = allObjects.map { blobName =>
          cloudStorageDirectory.blobPath match {
            case Some(bp) =>
              prefix.resolve(Paths.get(bp.asString).relativize(Paths.get(blobName.value)))
            case None =>
              prefix.resolve(Paths.get(blobName.value))
          }
        }

        allFiles.forall(_.toFile.exists()) shouldBe true
        allFiles.foreach(_.toFile.delete())
      }
      res.unsafeRunSync()
    }
  }

  "localizeCloudDirectory" should "only download files that match pattern" in {
    forAll { (cloudStorageDirectory: CloudStorageDirectory) =>
      val blob = cloudStorageDirectory.blobPath match {
        case Some(bp) => GcsBlobName(s"${bp.asString}/test.suffix")
        case None => GcsBlobName(s"test.suffix")
      }
      val blobNonExist = cloudStorageDirectory.blobPath match {
        case Some(bp) => GcsBlobName(s"${bp.asString}/random.txt")
        case None => GcsBlobName(s"random.txt")
      }
      val objectBody = genGcsObjectBody.sample.get
      val googleStorage = CloudStorageAlg.forGoogle(StorageAlgConfig(Paths.get("/tmp")), FakeGoogleStorageInterpreter)
      val workingDir = Paths.get("/tmp")
      val localBaseDir = RelativePath(Paths.get("edit"))

      val res = for {
        _ <- FakeGoogleStorageInterpreter.createBlob(cloudStorageDirectory.container.asGcsBucket, blob, objectBody, objectType).compile.drain
        _ <- FakeGoogleStorageInterpreter.createBlob(cloudStorageDirectory.container.asGcsBucket, blobNonExist, objectBody, objectType).compile.drain
        _ <- googleStorage
          .localizeCloudDirectory(localBaseDir, cloudStorageDirectory, workingDir, "suffix".r)
          .compile
          .drain
      } yield {
        val prefix = (workingDir.resolve(localBaseDir.asPath))
        val fileExist = cloudStorageDirectory.blobPath match {
          case Some(bp) =>
            prefix.resolve(Paths.get("test.suffix"))
          case None =>
            prefix.resolve(Paths.get("test.suffix"))
        }

        val fileNotExist = cloudStorageDirectory.blobPath match {
          case Some(bp) =>
            prefix.resolve(Paths.get("random.txt"))
          case None =>
            prefix.resolve(Paths.get("random.txt"))
        }

        assert(fileExist.toFile.exists())
        assert(!fileNotExist.toFile.exists())
        fileExist.toFile.delete()
      }
      res.unsafeRunSync()
    }
  }
}

object GoogleStorageServiceWithFailures extends BaseFakeGoogleStorage {
  override def streamUploadBlob(
      bucketName: GcsBucketName,
      objectName: GcsBlobName,
      metadata: Map[String, String],
      generation: Option[Long],
      overwrite: Boolean,
      traceId: Option[TraceId],
      blobWriteOptions: List[Storage.BlobWriteOption]
  ): Pipe[IO, Byte, Unit] = in => {
    val errors = new GoogleJsonError()
    errors.setCode(412)
    Stream.raiseError[IO](new com.google.cloud.storage.StorageException(errors))
  }
}
