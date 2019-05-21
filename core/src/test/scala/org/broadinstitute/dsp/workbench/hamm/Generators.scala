package org.broadinstitute.dsp.workbench.welder

import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.http4s.Uri
import org.scalacheck.Arbitrary

object Generators {
  val genGsPath = for {
    bucketName <- google2.Generators.genGcsBucketName
    objectName <- google2.Generators.genGcsBlobName
  } yield Uri.unsafeFromString(s"gs://$bucketName/a/$objectName")

  implicit val arbGsPath: Arbitrary[Uri] = Arbitrary(genGsPath)
  implicit val arbGcsBucketName: Arbitrary[GcsBucketName] = Arbitrary(google2.Generators.genGcsBucketName)
  implicit val arbGcsBlobName: Arbitrary[GcsBlobName] = Arbitrary(google2.Generators.genGcsBlobName)
}
