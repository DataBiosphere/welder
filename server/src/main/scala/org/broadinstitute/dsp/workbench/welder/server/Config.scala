package org.broadinstitute.dsp.workbench.welder
package server

import cats.implicits._
import org.http4s.Uri
import pureconfig.ConfigReader
import pureconfig.generic.auto._
import pureconfig.error.ExceptionThrown

object Config {
  implicit val uriConfigReader: ConfigReader[Uri] = ConfigReader.fromString(
    s => Uri.fromString(s).leftMap(err => ExceptionThrown(err))
  )
  val appConfig = pureconfig.loadConfig[AppConfig].leftMap(failures => new RuntimeException(failures.toList.map(_.description).mkString("\n")))
}

final case class AppConfig(pathToGoogleStorageCredentialJson: String)
