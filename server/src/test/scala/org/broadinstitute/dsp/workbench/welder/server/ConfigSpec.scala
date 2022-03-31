package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Paths

import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ConfigSpec extends AnyFlatSpec with Matchers {
  "Config" should "read configuration correctly" in {
    val config = Config.appConfig
    val objectServiceConfig = ObjectServiceConfig(Paths.get("/work"), WorkbenchEmail("fake@gmail.com"), 3 minutes)
    val expectedConfig = AppConfig(
      8080,
      7 minutes,
      3 minutes,
      15 minutes,
      GcsBlobName("welder-metadata/storage_links.json"),
      GcsBlobName("welder-metadata/gcs_metadata.json"),
      Paths.get(".delocalize.json"),
      objectServiceConfig,
      GcsBucketName("fakeBucket"),
      30 seconds,
      false
    )
    config shouldBe Right(expectedConfig)
  }
}
