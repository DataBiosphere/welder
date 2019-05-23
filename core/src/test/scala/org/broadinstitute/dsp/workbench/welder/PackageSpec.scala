package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Paths

import org.broadinstitute.dsde.workbench.google2.GcsBlobName
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

  "getSubPath" should "get relative path after base directory" in {
    val localPath = Paths.get("workspaces/ws1/sub/notebook1.ipynb")
    getLocalBaseDirectory(localPath) shouldBe(Right(Paths.get("workspaces/ws1")))
    getFullBlobName(localPath, BlobPath("notebooks")) shouldBe(Right(GcsBlobName("notebooks/sub/notebook1.ipynb")))
  }
}
