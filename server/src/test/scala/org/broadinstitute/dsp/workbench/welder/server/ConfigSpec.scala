package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Paths

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.scalatest.{FlatSpec, Matchers}
import scala.concurrent.duration._

class ConfigSpec extends FlatSpec with Matchers {
  "Config" should "read configuration correctly" in {
    val config = Config.appConfig
    val objectServiceConfig = ObjectServiceConfig(Paths.get("/work"), WorkbenchEmail("fake@gmail.com"), 20 minutes)
    val expectedConfig = AppConfig(8080, 7 minutes, 10 minutes, Paths.get("/work/storage_links.json"), Paths.get("/work/gcs_metadata.json"), objectServiceConfig)
    config shouldBe Right(expectedConfig)
  }
}
