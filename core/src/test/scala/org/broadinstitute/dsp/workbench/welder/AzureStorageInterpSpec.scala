package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.mtl.Ask
import com.azure.storage.blob.models.{BlobItem, ListBlobsOptions}
import fs2.Stream
import org.broadinstitute.dsde.workbench.azure.mock.FakeAzureStorageService
import org.broadinstitute.dsde.workbench.azure.{BlobName, ContainerName}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.{Path, Paths}

class AzureStorageInterpSpec extends AnyFlatSpec with WelderTestSuite {

  "gcsToLocalFile" should "be able to download a file from gcs and write to local path" in {
    forAll { (localObjectPath: RelativePath, azurePath: CloudBlobPath) =>
      val azureStorage = CloudStorageAlg.forAzure(
        StorageAlgConfig(Paths.get("/work")),
        new FakeAzureStorageService {
          override def downloadBlob(containerName: ContainerName, blobName: BlobName, path: Path, overwrite: Boolean)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
            createFile(path)
        }
      )
      val localAbsolutePath = Paths.get(s"/tmp/${localObjectPath.asPath.toString}")
      // Create the local base directory
      val directory = new File(s"${localAbsolutePath.getParent.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        resp <- azureStorage.gcsToLocalFile(localAbsolutePath, azurePath)
        file = new File(localAbsolutePath.toString)
        _ <- Stream.eval(IO.delay(file.exists() shouldBe true))
        _ <- Stream.eval(IO(file.delete()))
      } yield {
        resp shouldBe None
      }
      res.compile.drain.unsafeRunSync()
    }
  }

  "localizeCloudDirectory" should "recursively download files for a given CloudStorageDirectory" in {
    forAll { (cloudStorageDirectory: CloudStorageDirectory) =>
      val numFiles = 4
      val allObjects = Gen.listOfN(numFiles, genAzureBlobName).sample.get.map { x =>
        cloudStorageDirectory.blobPath match {
          case Some(bp) => BlobName(s"${bp.asString}/${x.value}")
          case None => BlobName(s"${x.value}")
        }
      }

      val azureStorage = CloudStorageAlg.forAzure(
        StorageAlgConfig(Paths.get("/tmp")),
        new FakeAzureStorageService {
          override def listObjects(containerName: ContainerName, opts: Option[ListBlobsOptions])(implicit ev: Ask[IO, TraceId]): Stream[IO, BlobItem] =
            Stream.emits(allObjects.map(n => new BlobItem().setName(n.value)))

          override def downloadBlob(containerName: ContainerName, blobName: BlobName, path: Path, overwrite: Boolean)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
            createFile(path)
        }
      )
      val workingDir = Paths.get("/tmp")
      val localBaseDir = RelativePath(Paths.get("edit"))

      val res = for {
        _ <- azureStorage.localizeCloudDirectory(localBaseDir, cloudStorageDirectory, workingDir, "".r).compile.drain
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
      val matchSuffix = "test.suffix"
      val blob1 = cloudStorageDirectory.blobPath match {
        case Some(bp) => BlobName(s"${bp.asString}/$matchSuffix")
        case None => BlobName(matchSuffix)
      }
      val nonMatchSuffix = "random.txt"
      val blob2 = cloudStorageDirectory.blobPath match {
        case Some(bp) => BlobName(s"${bp.asString}/$nonMatchSuffix")
        case None => BlobName(nonMatchSuffix)
      }

      val azureStorage = CloudStorageAlg.forAzure(
        StorageAlgConfig(Paths.get("/tmp")),
        new FakeAzureStorageService {
          override def listObjects(containerName: ContainerName, opts: Option[ListBlobsOptions])(implicit ev: Ask[IO, TraceId]): Stream[IO, BlobItem] =
            Stream.emits(List(new BlobItem().setName(blob1.value), new BlobItem().setName(blob2.value)))

          override def downloadBlob(containerName: ContainerName, blobName: BlobName, path: Path, overwrite: Boolean)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
            createFile(path)
        }
      )
      val workingDir = Paths.get("/tmp")
      val localBaseDir = RelativePath(Paths.get("edit"))

      val res = for {
        _ <- azureStorage
          .localizeCloudDirectory(localBaseDir, cloudStorageDirectory, workingDir, "suffix".r)
          .compile
          .drain
      } yield {
        val prefix = (workingDir.resolve(localBaseDir.asPath))
        val fileExist = cloudStorageDirectory.blobPath match {
          case Some(bp) =>
            prefix.resolve(Paths.get(matchSuffix))
          case None =>
            prefix.resolve(Paths.get(matchSuffix))
        }

        val fileNotExist = cloudStorageDirectory.blobPath match {
          case Some(bp) =>
            prefix.resolve(Paths.get(nonMatchSuffix))
          case None =>
            prefix.resolve(Paths.get(nonMatchSuffix))
        }

        assert(fileExist.toFile.exists())
        assert(!fileNotExist.toFile.exists())
        fileExist.toFile.delete()
      }
      res.unsafeRunSync()
    }
  }

  private def createFile(path: Path): IO[Unit] =
    for {
      file <- IO(new File(path.toAbsolutePath.toUri.getPath))
      _ <- IO(file.createNewFile())
    } yield ()
}

object FakeAzureStorageInterp extends FakeAzureStorageService
