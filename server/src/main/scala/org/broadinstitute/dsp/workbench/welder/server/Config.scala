package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path
import java.nio.file.Paths

import cats.implicits._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.http4s.Uri
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.auto._
import pureconfig.error.ExceptionThrown

import scala.concurrent.duration.FiniteDuration

object Config {
  implicit val uriConfigReader: ConfigReader[Uri] = ConfigReader.fromString(s => Uri.fromString(s).leftMap(err => ExceptionThrown(err)))
  implicit val pathConfigReader: ConfigReader[Path] = ConfigReader.fromString(s => Either.catchNonFatal(Paths.get(s)).leftMap(err => ExceptionThrown(err)))
  implicit val relativePathConfigReader: ConfigReader[RelativePath] = pathConfigReader.map(RelativePath)
  implicit val workbenchEmailConfigReader: ConfigReader[WorkbenchEmail] = ConfigReader.stringConfigReader.map(WorkbenchEmail)
  implicit val gcsBlobNameReader: ConfigReader[GcsBlobName] = ConfigReader.stringConfigReader.map(GcsBlobName)
  implicit val gcsBucketNameConfigReader: ConfigReader[GcsBucketName] = ConfigReader.stringConfigReader.map(GcsBucketName)

  val appConfig = ConfigSource.default.load[AppConfig].leftMap(failures => new RuntimeException(failures.toList.map(_.description).mkString("\n")))
}

final case class AppConfig(
    serverPort: Int,
    cleanUpLockInterval: FiniteDuration,
    flushCacheInterval: FiniteDuration,
    syncCloudStorageDirectoryInterval: FiniteDuration,
    storageLinksJsonBlobName: GcsBlobName,
    gcsMetadataJsonBlobName: GcsBlobName,
    workspaceBucketNameFileName: Path,
    objectService: ObjectServiceConfig,
    stagingBucketName: GcsBucketName
)

final case class EnvironmentVariables(currentUser: WorkbenchEmail)
