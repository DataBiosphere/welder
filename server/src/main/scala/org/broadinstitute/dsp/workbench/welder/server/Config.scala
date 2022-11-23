package org.broadinstitute.dsp.workbench.welder
package server

import cats.implicits._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.http4s.Uri
import pureconfig.error.ExceptionThrown
import pureconfig.generic.auto._
import pureconfig.{ConfigReader, ConfigSource}

import java.nio.file.{Path, Paths}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

object Config {
  implicit val uriConfigReader: ConfigReader[Uri] = ConfigReader.fromString(s => Uri.fromString(s).leftMap(err => ExceptionThrown(err)))
  implicit val pathConfigReader: ConfigReader[Path] = ConfigReader.fromString(s => Either.catchNonFatal(Paths.get(s)).leftMap(err => ExceptionThrown(err)))
  implicit val relativePathConfigReader: ConfigReader[RelativePath] = pathConfigReader.map(RelativePath)
  implicit val workbenchEmailConfigReader: ConfigReader[WorkbenchEmail] = ConfigReader.stringConfigReader.map(WorkbenchEmail)
  implicit val gcsBlobNameReader: ConfigReader[GcsBlobName] = ConfigReader.stringConfigReader.map(GcsBlobName)
  implicit val gcsBucketNameConfigReader: ConfigReader[GcsBucketName] = ConfigReader.stringConfigReader.map(GcsBucketName)
  implicit val storageBlobConfigReader: ConfigReader[CloudStorageBlob] = ConfigReader.stringConfigReader.map(CloudStorageBlob)
  implicit val storageContainerConfigReader: ConfigReader[CloudStorageContainer] = ConfigReader.stringConfigReader.map(CloudStorageContainer)

  // pureconfig's auto generated ConfigReader will read AppConfig as a sealed class depending on `type` value in config
  val appConfig = ConfigSource.default.load[AppConfig].leftMap(failures => new RuntimeException(failures.toList.map(_.description).mkString("\n")))
}

sealed trait AppConfig extends Product with Serializable {
  def cloudProvider: CloudProvider

  def serverPort: Int
  def cleanUpLockInterval: FiniteDuration
  def flushCacheInterval: FiniteDuration
  def syncCloudStorageDirectoryInterval: FiniteDuration
  def storageLinksJsonBlobName: CloudStorageBlob
  def metadataJsonBlobName: CloudStorageBlob
  def workspaceBucketNameFileName: Path
  def objectService: ObjectServiceConfig
  def stagingBucketName: CloudStorageContainer
  def delocalizeDirectoryInterval: FiniteDuration
  def shouldBackgroundSync: Boolean

  def getStorageLinksJsonUri: CloudBlobPath = CloudBlobPath(stagingBucketName, storageLinksJsonBlobName)
  def getMetadataJsonBlobNameUri: CloudBlobPath = CloudBlobPath(stagingBucketName, metadataJsonBlobName)
}
object AppConfig {
  final case class Gcp(
      serverPort: Int,
      cleanUpLockInterval: FiniteDuration,
      flushCacheInterval: FiniteDuration,
      syncCloudStorageDirectoryInterval: FiniteDuration,
      storageLinksJsonBlobName: CloudStorageBlob,
      metadataJsonBlobName: CloudStorageBlob,
      workspaceBucketNameFileName: Path,
      objectService: ObjectServiceConfig,
      stagingBucketName: CloudStorageContainer,
      delocalizeDirectoryInterval: FiniteDuration,
      shouldBackgroundSync: Boolean
  ) extends AppConfig {
    override def cloudProvider: CloudProvider = CloudProvider.Gcp
  }

  final case class Azure(
      serverPort: Int,
      cleanUpLockInterval: FiniteDuration,
      flushCacheInterval: FiniteDuration,
      syncCloudStorageDirectoryInterval: FiniteDuration,
      storageLinksJsonBlobName: CloudStorageBlob,
      metadataJsonBlobName: CloudStorageBlob,
      workspaceBucketNameFileName: Path,
      objectService: ObjectServiceConfig,
      stagingBucketName: CloudStorageContainer,
      delocalizeDirectoryInterval: FiniteDuration,
      miscHttpClientConfig: MiscHttpClientConfig,
      shouldBackgroundSync: Boolean,
      workspaceStorageContainerResourceId: UUID,
      stagingStorageContainerResourceId: UUID
  ) extends AppConfig {
    override def cloudProvider: CloudProvider = CloudProvider.Azure
  }
}

final case class EnvironmentVariables(currentUser: WorkbenchEmail)
