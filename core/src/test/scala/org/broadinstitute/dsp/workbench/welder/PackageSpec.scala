package org.broadinstitute.dsp.workbench.welder

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.azure.{ContainerName, EndpointUrl}

import java.nio.file.Paths
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.Generators.arbGcsBucketName
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PackageSpec extends AnyFlatSpec with ScalaCheckPropertyChecks with WelderTestSuite {
  "parseGsPath" should "be able to parse gs path correctly" in {
    forAll { (bucketName: GcsBucketName) =>
      val gsPath = s"gs://${bucketName.value}/notebooks/sub/1.ipynb"
      parseGsPath(gsPath) shouldBe (Right(GsPath(bucketName, GcsBlobName("notebooks/sub/1.ipynb"))))
    }
  }

  it should "fail to parse gs path when input is invalid" in {
    forAll { (bucketName: GcsBucketName) =>
      val gsPath = s"gs://${bucketName.value}"
      parseGsPath(gsPath) shouldBe (Left("objectName can't be empty"))

      val gsPath2 = s"gs:///"
      parseGsPath(gsPath2) shouldBe (Left("failed to parse bucket name"))

      val gsPath3 = s"invalidgs://"
      parseGsPath(gsPath3) shouldBe (Left("gs directory has to be prefixed with gs://"))
    }
  }

  "getFullBlobName" should "get object name in gcs" in {
    val localPath = Paths.get("workspaces/ws1/sub/notebook1.ipynb")
    getPossibleBaseDirectory(localPath).map(_.toString) shouldBe (List("workspaces/ws1/sub", "workspaces/ws1", "workspaces"))
    val basePath = RelativePath(Paths.get("workspaces/ws1"))
    getFullBlobName(basePath, localPath, Some(BlobPath("notebooks"))) shouldBe (GcsBlobName("notebooks/sub/notebook1.ipynb"))
  }

  it should "parse path correctly when blobPath doesn't exist" in {
    val localPath = Paths.get("workspaces/ws1/sub/notebook1.ipynb")
    val basePath = RelativePath(Paths.get("workspaces/ws1"))
    getFullBlobName(basePath, localPath, None) shouldBe (GcsBlobName("sub/notebook1.ipynb"))
  }

  "getLocalPath" should "calculate local path correctly when blobPath exists" in {
    val baseDirectory = RelativePath(Paths.get("edit"))
    val blobPath = Some(BlobPath("directory"))
    val blobName = "directory/a/blob1"
    getLocalPath(baseDirectory, blobPath, blobName) shouldBe (Right(RelativePath(Paths.get("edit/a/blob1"))))
  }

  it should "calculate local path correctly when blobPath is None" in {
    val baseDirectory = RelativePath(Paths.get("edit"))
    val blobPath = None
    val blobName = "directory/a/blob1"
    getLocalPath(baseDirectory, blobPath, blobName) shouldBe (Right(RelativePath(Paths.get("edit/directory/a/blob1"))))
  }

  "hashMetadata" should "consistently hash a string" in {
    val knownHash = HashedLockedBy("4af48213b034805aacef2309fe802d97f2fbbfcd2ea5a641988b015e9855f394") //decodes to "test-bucket:foo@bar.com"
    val bucketName = GcsBucketName("test-bucket")
    val email = WorkbenchEmail("foo@bar.com")
    hashString(bucketName.value + ":" + email.value) shouldBe Right(knownHash)
  }

  "cachedResource" should "load empty cache if it doesn't exist in both local disk and gcs" in {
    forAll { (gcsBucketName: GcsBucketName) =>
      val googleStorageAlg = CloudStorageAlg.forGoogle(StorageAlgConfig(Paths.get("/tmp")), FakeGoogleStorageInterpreter)
      val res =
        cachedResource[String, String](
          Ref.of[IO, CloudStorageAlg](googleStorageAlg).unsafeRunSync(),
          SourceUri.GsPath(gcsBucketName, GcsBlobName("welder-metadata/storage_links.json")),
          s => List((s, s))
        ).compile.lastOrError
          .unsafeRunSync()
      res.get.unsafeRunSync() shouldBe Map.empty
    }
  }

  "findFilesWithPattern" should "only find files with certain pattern" in {
    val parentPath = Paths.get("/tmp")
    new java.io.File(parentPath.resolve("test.R").toUri).createNewFile()
    new java.io.File(parentPath.resolve("test.R1").toUri).createNewFile()
    new java.io.File(parentPath.resolve("test.Rmd").toUri).createNewFile()
    new java.io.File(parentPath.resolve("t.R").toUri).createNewFile()
    new java.io.File(parentPath.resolve("abR").toUri).createNewFile()
    new java.io.File(parentPath.resolve("test.ipynb").toUri).createNewFile()
    val resR = findFilesWithPattern(parentPath, ".+(\\.R|\\.Rmd)$".r)
    resR.map(_.toString) should contain theSameElementsAs List("/tmp/test.R", "/tmp/test.Rmd", "/tmp/t.R")

    val resIpynb = findFilesWithPattern(parentPath, ".*.ipynb$".r)
    resIpynb.map(_.toString) shouldBe List("/tmp/test.ipynb")
  }

  "getStorageContainerNameFromUrl" should "parse container name properly" in {
    val sasUrl =
      EndpointUrl(
        "https://sa442c39879ef8ba0c74cd.blob.core.windows.net/ls-saturn-72ca613f-eb77-4bb0-afea-88ed5ef1da85?sp=r&st=2022-08-17T18:59:32Z&se=2022-08-18T02:59:32Z&spr=https&sv=2021-06-08&sr=c&sig=Qmv2ExJwkVtOuSoxi%"
      )
    getStorageContainerNameFromUrl(sasUrl) shouldBe (Right(ContainerName("ls-saturn-72ca613f-eb77-4bb0-afea-88ed5ef1da85")))
  }
}
