package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Paths

import org.scalatest.{FlatSpec, Matchers}

class ConfigSpec extends FlatSpec with Matchers {
  "Config" should "read configuration correctly" in {
    val config = Config.appConfig
    val expectedConfig = AppConfig("fakePath", Paths.get("fakeStorageLinksPath"))
    config shouldBe Right(expectedConfig)
  }
}
