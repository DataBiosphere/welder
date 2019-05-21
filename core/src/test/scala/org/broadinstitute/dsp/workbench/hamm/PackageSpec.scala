package org.broadinstitute.dsp.workbench.welder

import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.Generators.{arbGcsBlobName, arbGcsBucketName}
import org.scalatest.FlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PackageSpec extends FlatSpec with ScalaCheckPropertyChecks with WelderTestSuite {
  "parseGsDirectory" should "be able to parse gs path correctly" in {
    forAll{
      (bucketName: GcsBucketName, objectName: GcsBlobName) =>
        val gsPath = s"gs://${bucketName.value}/a/${objectName.value}"
        parseGsDirectory(gsPath) shouldBe(Right(BucketNameAndObjectName(bucketName, objectName.copy(s"a/${objectName.value}"))))
    }
  }

  "parseGsDirectory" should "fail to parse gs path when input is invalid" in {
    forAll{
      (bucketName: GcsBucketName) =>
        val gsPath = s"gs://${bucketName.value}"
        parseGsDirectory(gsPath) shouldBe(Left("objectName can't be empty"))

        val gsPath2 = s"gs:///"
        parseGsDirectory(gsPath2) shouldBe(Left("failed to parse bucket name"))
    }
  }
}
