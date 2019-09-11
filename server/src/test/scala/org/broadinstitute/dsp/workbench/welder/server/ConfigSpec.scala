package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Paths

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.scalatest.{FlatSpec, Matchers}
import scala.concurrent.duration._

class ConfigSpec extends FlatSpec with Matchers {
  "Config" should "read configuration correctly" in {
    val config = Config.appConfig
    val objectServiceConfig = ObjectServiceConfig(Paths.get("/work"), WorkbenchEmail("fake@gmail.com"), 3 minutes)
    val expectedConfig = AppConfig(8080, 7 minutes, 10 minutes, 15 minutes, Paths.get("/work/.welder/storage_links.json"), Paths.get("/work/.welder/gcs_metadata.json"), Paths.get(".delocalize.json"), objectServiceConfig)
    config shouldBe Right(expectedConfig)
  }
}
