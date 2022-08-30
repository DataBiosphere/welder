package org.broadinstitute.dsp.workbench.welder

import org.broadinstitute.dsde.workbench.azure.{BlobName, ContainerName}
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.http4s.Uri
import org.scalacheck.{Arbitrary, Gen}

import java.nio.file.{Path, Paths}

object Generators {
  val genGsPathUri = for {
    bucketName <- google2.Generators.genGcsBucketName
    objectName <- google2.Generators.genGcsBlobName
  } yield Uri.unsafeFromString(s"gs://${bucketName.value}/a/${objectName.value}")

  val genCloudStorageContainer = google2.Generators.genGcsBucketName.map(b => CloudStorageContainer(b.value))
  val genCloudStorageBlob = google2.Generators.genGcsBucketName.map(b => CloudStorageBlob(b.value))
  val genCloudBlobPath = for {
    bucketName <- genCloudStorageContainer
    objectName <- genCloudStorageBlob
  } yield CloudBlobPath(bucketName, objectName)

  val genAzureBlobName: Gen[BlobName] = Gen.uuid.map(x => BlobName(s"blob${x.toString}"))
  val genAzureContainerName: Gen[ContainerName] = Gen.uuid.map(x => ContainerName(s"container${x.toString}"))
  val genAzureFileContent: Gen[String] = Gen.stringOfN(10, Gen.alphaLowerChar)

  val genFilePath = Gen.uuid.map(uuid => Paths.get(s"dir/${uuid}"))
  val genRelativePath = Gen.uuid.map(uuid => RelativePath(Paths.get(s"dir/${uuid}")))
  val genLocalBaseDirectory = for {
    workspaceName <- Gen.uuid
  } yield LocalBaseDirectory(RelativePath(Paths.get(s"workspaces/$workspaceName")))
  val genLocalPathWithSubDirectory = for {
    workspaceName <- Gen.uuid
    fileName <- Gen.uuid
  } yield Paths.get(s"workspaces/$workspaceName/sub/$fileName")
  val genLocalSafeBaseDirectory = for {
    workspaceName <- Gen.uuid
  } yield LocalSafeBaseDirectory(RelativePath(Paths.get(s"workspaces_safe/$workspaceName")))
  val genLocalPathScratch = for {
    fileName <- Gen.uuid
  } yield Paths.get(s"scratch/$fileName")

  val genCloudStorageDirectory = for {
    bucketName <- google2.Generators.genGcsBucketName
    blobPath = Gen.uuid.map(x => BlobPath(x.toString))
    blobPathOpt <- Gen.option[BlobPath](blobPath)
  } yield CloudStorageDirectory(CloudStorageContainer(bucketName.value), blobPathOpt)

  val genIpynbStorageLink = for {
    localBaseDirectory <- genLocalBaseDirectory
    localSafeDirectory <- genLocalSafeBaseDirectory
    cloudStorageDirectory <- genCloudStorageDirectory
  } yield StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb$".r)

  val genRmdStorageLink = for {
    localBaseDirectory <- genLocalBaseDirectory
    cloudStorageDirectory <- genCloudStorageDirectory
  } yield StorageLink(localBaseDirectory, None, cloudStorageDirectory, "\\.Rmd$".r)

  val genRmdFile = for {
    fileName <- Gen.uuid
  } yield Paths.get(s"/tmp/${fileName}.Rmd")

  val genWorkbenchEmail = Gen.uuid.map(x => WorkbenchEmail(s"$x@gmail.com"))

  implicit val arbGsPathUri: Arbitrary[Uri] = Arbitrary(genGsPathUri)
  implicit val arbGsPath: Arbitrary[CloudBlobPath] = Arbitrary(genCloudBlobPath)
  implicit val arbCloudStorageContainer: Arbitrary[CloudStorageContainer] = Arbitrary(genCloudStorageContainer)
  implicit val arbGcsBucketName: Arbitrary[GcsBucketName] = Arbitrary(google2.Generators.genGcsBucketName)
  implicit val arbGcsBlobName: Arbitrary[GcsBlobName] = Arbitrary(google2.Generators.genGcsBlobName)
  implicit val arbFilePath: Arbitrary[Path] = Arbitrary(genFilePath)
  implicit val arbRelativePathPath: Arbitrary[RelativePath] = Arbitrary(genRelativePath)
  implicit val arbCloudStorageDirectory: Arbitrary[CloudStorageDirectory] = Arbitrary(genCloudStorageDirectory)
  implicit val arbLocalBaseDirectory: Arbitrary[LocalBaseDirectory] = Arbitrary(genLocalBaseDirectory)
  implicit val arbLocalSafeBaseDirectory: Arbitrary[LocalSafeBaseDirectory] = Arbitrary(genLocalSafeBaseDirectory)
  implicit val arbWorkbenchEmail: Arbitrary[WorkbenchEmail] = Arbitrary(genWorkbenchEmail)
  implicit val arbIpynbStorageLink: Arbitrary[StorageLink] = Arbitrary(genIpynbStorageLink)
}
