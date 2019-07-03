package org.broadinstitute.dsp.workbench.welder

import io.circe.Json
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.Generators.arbGcsBucketName
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.scalatest.FlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class JsonCodecSpec extends FlatSpec with ScalaCheckPropertyChecks with WelderTestSuite {
  "cloudStorageDirectoryDecoder" should "be able to parse cloudStorageDirectory correctly" in {
    forAll {
      (bucketName: GcsBucketName) =>
        val inputString = s"gs://${bucketName.value}/notebooks/sub"

        val res = Json.fromString(inputString).as[CloudStorageDirectory]
        res shouldBe(Right(CloudStorageDirectory(bucketName, Some(BlobPath("notebooks/sub")))))
    }
  }

  it should "be able to parse cloudStorageDirectory correctly when blobPath is empty" in {
    forAll {
      (bucketName: GcsBucketName) =>
        val inputString = s"gs://${bucketName.value}"

        val res = Json.fromString(inputString).as[CloudStorageDirectory]
        res shouldBe(Right(CloudStorageDirectory(bucketName, None)))
    }
  }
}
