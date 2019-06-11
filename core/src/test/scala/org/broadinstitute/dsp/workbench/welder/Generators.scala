package org.broadinstitute.dsp.workbench.welder

import java.nio.file.{Path, Paths}

import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.http4s.Uri
import org.scalacheck.{Arbitrary, Gen}

object Generators {
  val genGsPath = for {
    bucketName <- google2.Generators.genGcsBucketName
    objectName <- google2.Generators.genGcsBlobName
  } yield Uri.unsafeFromString(s"gs://$bucketName/a/$objectName")

  val genFilePath = Gen.uuid.map(uuid => Paths.get(s"/tmp/${uuid}"))
  val genLocalBaseDirectory = for {
    workspaceName <- Gen.uuid
  } yield LocalBaseDirectory(Paths.get(s"workspaces/$workspaceName"))
  val genLocalPathWithSubDirectory = for {
    workspaceName <- Gen.uuid
    fileName <- Gen.uuid
  } yield Paths.get(s"workspaces/$workspaceName/sub/$fileName")
  val genLocalSafeBaseDirectory = for {
    workspaceName <- Gen.uuid
  } yield LocalSafeBaseDirectory(Paths.get(s"workspaces_safe/$workspaceName"))
  val genLocalPathScratch = for {
    fileName <- Gen.uuid
  } yield Paths.get(s"scratch/$fileName")

  val genCloudStorageDirectory = for {
    bucketName <- google2.Generators.genGcsBucketName
    blobPath <- Gen.uuid.map(x => BlobPath(x.toString))
  } yield CloudStorageDirectory(bucketName, blobPath)

  val genWorkbenchEmail = Gen.uuid.map(x => WorkbenchEmail(s"$x@gmail.com"))

  implicit val arbGsPath: Arbitrary[Uri] = Arbitrary(genGsPath)
  implicit val arbGcsBucketName: Arbitrary[GcsBucketName] = Arbitrary(google2.Generators.genGcsBucketName)
  implicit val arbGcsBlobName: Arbitrary[GcsBlobName] = Arbitrary(google2.Generators.genGcsBlobName)
  implicit val arbFilePath: Arbitrary[Path] = Arbitrary(genFilePath)
  implicit val arbCloudStorageDirectory: Arbitrary[CloudStorageDirectory] = Arbitrary(genCloudStorageDirectory)
  implicit val arbLocalBaseDirectory: Arbitrary[LocalBaseDirectory] = Arbitrary(genLocalBaseDirectory)
  implicit val arbLocalSafeBaseDirectory: Arbitrary[LocalSafeBaseDirectory] = Arbitrary(genLocalSafeBaseDirectory)
  implicit val arbWorkbenchEmail: Arbitrary[WorkbenchEmail] = Arbitrary(genWorkbenchEmail)
}
