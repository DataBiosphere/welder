package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Paths
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.http4s.Uri
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration._

class ConfigSpec extends AnyFlatSpec with Matchers {
  "Config" should "read configuration correctly" in {
    val config = Config.appConfig
    val objectServiceConfig = ObjectServiceConfig(Paths.get("/work"), WorkbenchEmail("fake@gmail.com"), 3 minutes)
    val expectedConfig = AppConfig(
      8080,
      7 minutes,
      1 minutes,
      15 minutes,
      GcsBlobName("welder-metadata/storage_links.json"),
      GcsBlobName("welder-metadata/gcs_metadata.json"),
      Paths.get(".delocalize.json"),
      objectServiceConfig,
      GcsBucketName("fakeBucket"),
      30 seconds,
      MiscHttpClientConfig(
        Uri.unsafeFromString("https://workspace.dsde-dev.broadinstitute.org/"),
        UUID.fromString("a5a1f1e1-bcb0-49d9-b589-ea4d7c9d6f02"),
        UUID.fromString("9151f3a0-fe5c-49c5-b8a1-dc15cd96b174")
      ),
      false,
      CloudProvider.Gcp
    )
    config shouldBe Right(expectedConfig)
  }
}
