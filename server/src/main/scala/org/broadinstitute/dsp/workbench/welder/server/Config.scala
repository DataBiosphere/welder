package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path
import java.nio.file.Paths

import cats.implicits._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.http4s.Uri
import pureconfig.ConfigReader
import pureconfig.generic.auto._
import pureconfig.error.ExceptionThrown

import scala.concurrent.duration.FiniteDuration

object Config {
  implicit val uriConfigReader: ConfigReader[Uri] = ConfigReader.fromString(
    s => Uri.fromString(s).leftMap(err => ExceptionThrown(err))
  )
  implicit val pathConfigReader: ConfigReader[Path] = ConfigReader.fromString(
    s => Either.catchNonFatal(Paths.get(s)).leftMap(err => ExceptionThrown(err))
  )
  implicit val workbenchEmailConfigReader: ConfigReader[WorkbenchEmail] = ConfigReader.fromString(
    s => Right(WorkbenchEmail(s))
  )

  val appConfig = pureconfig.loadConfig[AppConfig].leftMap(failures => new RuntimeException(failures.toList.map(_.description).mkString("\n")))
}

final case class AppConfig(
    serverPort: Int,
    cleanUpLockInterval: FiniteDuration,
    flushCacheInterval: FiniteDuration,
    syncCloudStorageDirectoryInterval: FiniteDuration,
    pathToStorageLinksJson: Path,
    pathToGcsMetadataJson: Path,
    objectService: ObjectServiceConfig
)

final case class EnvironmentVariables(currentUser: WorkbenchEmail)
