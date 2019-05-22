package org.broadinstitute.dsp.workbench.welder
package server

import org.scalatest.{FlatSpec, Matchers}

class ConfigSpec extends FlatSpec with Matchers {
  "Config" should "read configuration correctly" in {
    val config = Config.appConfig
    val expectedConfig = AppConfig("fakePath", "storageLinks.json")
    config shouldBe Right(expectedConfig)
  }
}
