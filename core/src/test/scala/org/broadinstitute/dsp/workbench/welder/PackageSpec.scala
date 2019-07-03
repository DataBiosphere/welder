package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Paths

import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.Generators.arbGcsBucketName
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.scalatest.FlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PackageSpec extends FlatSpec with ScalaCheckPropertyChecks with WelderTestSuite {
  "parseGsPath" should "be able to parse gs path correctly" in {
    forAll {
      (bucketName: GcsBucketName) =>
        val gsPath = s"gs://${bucketName.value}/notebooks/sub/1.ipynb"
        parseGsPath(gsPath) shouldBe(Right(GsPath(bucketName, GcsBlobName("notebooks/sub/1.ipynb"))))
    }
  }

  it should "fail to parse gs path when input is invalid" in {
    forAll {
      (bucketName: GcsBucketName) =>
        val gsPath = s"gs://${bucketName.value}"
        parseGsPath(gsPath) shouldBe(Left("objectName can't be empty"))

        val gsPath2 = s"gs:///"
        parseGsPath(gsPath2) shouldBe(Left("failed to parse bucket name"))

        val gsPath3 = s"invalidgs://"
        parseGsPath(gsPath3) shouldBe(Left("gs directory has to be prefixed with gs://"))
    }
  }

  "getFullBlobName" should "get object name in gcs" in {
    val localPath = Paths.get("workspaces/ws1/sub/notebook1.ipynb")
    getPossibleBaseDirectory(localPath).map(_.toString) shouldBe(List("workspaces/ws1/sub", "workspaces/ws1", "workspaces"))
    val basePath = RelativePath(Paths.get("workspaces/ws1"))
    getFullBlobName(basePath, localPath, Some(BlobPath("notebooks"))) shouldBe(GcsBlobName("notebooks/sub/notebook1.ipynb"))
  }

  it should "parse path correctly when blobPath doesn't exist" in {
    val localPath = Paths.get("workspaces/ws1/sub/notebook1.ipynb")
    val basePath = RelativePath(Paths.get("workspaces/ws1"))
    getFullBlobName(basePath, localPath, None) shouldBe(GcsBlobName("sub/notebook1.ipynb"))
  }

  "hashMetadata" should "consistently hash a string" in {
    val knownHash = HashedLockedBy("4af48213b034805aacef2309fe802d97f2fbbfcd2ea5a641988b015e9855f394") //decodes to "test-bucket:foo@bar.com"
    val bucketName = GcsBucketName("test-bucket")
    val email = WorkbenchEmail("foo@bar.com")
    hashString(bucketName.value + ":" + email.value) shouldBe Right(knownHash)
  }
}
