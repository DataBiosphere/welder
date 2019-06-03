package org.broadinstitute.dsp.workbench.welder

import java.nio.file.{Path, Paths}

import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.http4s.Uri
import org.scalacheck.{Arbitrary, Gen}

object Generators {
  val genGsPath = for {
    bucketName <- google2.Generators.genGcsBucketName
    objectName <- google2.Generators.genGcsBlobName
  } yield Uri.unsafeFromString(s"gs://$bucketName/a/$objectName")

  val genFilePath = Gen.uuid.map(uuid => Paths.get(s"/tmp/${uuid}"))
  val genLocalPath = for {
    workspaceName <- Gen.uuid
    fileName <- Gen.uuid
  } yield Paths.get(s"workspaces/$workspaceName/$fileName")
  val genLocalPathWithSubDirectory = for {
    workspaceName <- Gen.uuid
    fileName <- Gen.uuid
  } yield Paths.get(s"workspaces/$workspaceName/sub/$fileName")
  val genLocalPathSafeMode = for {
    workspaceName <- Gen.uuid
    fileName <- Gen.uuid
  } yield Paths.get(s"workspaces_safe/$workspaceName/$fileName")
  val genLocalPathScratch = for {
    fileName <- Gen.uuid
  } yield Paths.get(s"scratch/$fileName")

  implicit val arbGsPath: Arbitrary[Uri] = Arbitrary(genGsPath)
  implicit val arbGcsBucketName: Arbitrary[GcsBucketName] = Arbitrary(google2.Generators.genGcsBucketName)
  implicit val arbGcsBlobName: Arbitrary[GcsBlobName] = Arbitrary(google2.Generators.genGcsBlobName)
  implicit val arbFilePath: Arbitrary[Path] = Arbitrary(genFilePath)
}
