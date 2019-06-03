package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Paths

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class ConfigSpec extends FlatSpec with Matchers {
  "Config" should "read configuration correctly" in {
    val config = Config.appConfig
    val expectedConfig = AppConfig(Paths.get("/home/jupyter-user/storage_links.json"), WorkbenchEmail("fake@gmail.com"), Paths.get("/home/jupyter-user"), 20 minutes)
    config shouldBe Right(expectedConfig)
  }
}
