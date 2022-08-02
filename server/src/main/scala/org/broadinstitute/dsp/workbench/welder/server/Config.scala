package org.broadinstitute.dsp.workbench.welder
package server

import ca.mrvisser.sealerate

import java.nio.file.Path
import java.nio.file.Paths
import cats.implicits._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.http4s.Uri
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.auto._
import pureconfig.error.{CannotConvert, ExceptionThrown}

import scala.concurrent.duration.FiniteDuration

object Config {
  implicit val uriConfigReader: ConfigReader[Uri] = ConfigReader.fromString(s => Uri.fromString(s).leftMap(err => ExceptionThrown(err)))
  implicit val pathConfigReader: ConfigReader[Path] = ConfigReader.fromString(s => Either.catchNonFatal(Paths.get(s)).leftMap(err => ExceptionThrown(err)))
  implicit val relativePathConfigReader: ConfigReader[RelativePath] = pathConfigReader.map(RelativePath)
  implicit val workbenchEmailConfigReader: ConfigReader[WorkbenchEmail] = ConfigReader.stringConfigReader.map(WorkbenchEmail)
  implicit val gcsBlobNameReader: ConfigReader[GcsBlobName] = ConfigReader.stringConfigReader.map(GcsBlobName)
  implicit val gcsBucketNameConfigReader: ConfigReader[GcsBucketName] = ConfigReader.stringConfigReader.map(GcsBucketName)
  implicit val cloudProviderReader: ConfigReader[CloudProvider] =
    ConfigReader.stringConfigReader.emap(s =>
      CloudProvider.stringToCloudProvider.get(s).toRight(CannotConvert(s, "CloudProvider", s"valid values: GCP, AZURE"))
    )

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
    stagingBucketName: GcsBucketName,
    delocalizeDirectoryInterval: FiniteDuration,
    miscHttpClientConfig: MiscHttpClientConfig,
    isRstudioRuntime: Boolean,
    cloudProvider: CloudProvider
)

final case class EnvironmentVariables(currentUser: WorkbenchEmail)
sealed abstract class CloudProvider extends Product with Serializable {
  def asString: String
}
object CloudProvider {
  final case object Gcp extends CloudProvider {
    override val asString = "GCP"
  }
  final case object Azure extends CloudProvider {
    override val asString = "AZURE"
  }

  val stringToCloudProvider = sealerate.values[CloudProvider].map(p => (p.asString, p)).toMap
}
