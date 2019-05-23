package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path
import java.nio.file.Paths

import cats.effect.IO
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

  val appConfig = pureconfig.loadConfig[AppConfig].leftMap(failures => new RuntimeException(failures.toList.map(_.description).mkString("\n")))

  def readEnvironmentVariables: IO[EnvironmentVariables] = {
    IO(System.getenv("OWNER_EMAIL")).map(s => EnvironmentVariables(WorkbenchEmail(s)))
  }
}

final case class AppConfig(
                           pathToStorageLinksJson: Path,
                           workingDirectory: Path, //root directory where all local files will be mounted
                           lockExpiration: FiniteDuration
                          )

final case class EnvironmentVariables(currentUser: WorkbenchEmail)