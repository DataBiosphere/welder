package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Paths
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.http4s.Uri
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

import java.util.UUID
//import org.broadinstitute.dsp.workbench.welder.server.Config.appConfigConvert
import scala.concurrent.duration._

class ConfigSpec extends AnyFlatSpec with Matchers {
  it should "read GCP configuration correctly" in {
    import pureconfig.generic.auto._
    import org.broadinstitute.dsp.workbench.welder.server.Config._
    val objectServiceConfig = ObjectServiceConfig(Paths.get("/work"), WorkbenchEmail("fake@gmail.com"), 3 minutes)

    val configString =
      """
        |server-port = 8080
        |storage-links-json-blob-name = "welder-metadata/storage_links.json"
        |metadata-json-blob-name = "welder-metadata/gcs_metadata.json"
        |workspace-bucket-name-file-name = ".delocalize.json"
        |working-directory = "/tmp"
        |is-rstudio-runtime = false
        |object-service {
        |  working-directory = "/work"
        |  lock-expiration = 3 minutes
        |  owner-email = "fake@gmail.com"
        |}
        |
        |staging-bucket-name = "fakeBucket"
        |clean-up-lock-interval = 7 minutes
        |flush-cache-interval = 1 minutes
        |sync-cloud-storage-directory-interval = 15 minutes
        |delocalize-directory-interval = 30 seconds
        |is-rstudio-runtime = false
        |# value needs to be lower case
        |type = "gcp"
        |""".stripMargin

    val expectedConfig = AppConfig.Gcp(
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
      false
    )
    ConfigSource.string(configString).load[AppConfig.Gcp] shouldBe Right(expectedConfig)
  }

  it should "read Azure configuration correctly" in {
    import pureconfig.generic.auto._
    import org.broadinstitute.dsp.workbench.welder.server.Config._
    val objectServiceConfig = ObjectServiceConfig(Paths.get("/work"), WorkbenchEmail("fake@gmail.com"), 3 minutes)

    val configString =
      """
        |server-port = 8080
        |storage-links-json-blob-name = "welder-metadata/storage_links.json"
        |metadata-json-blob-name = "welder-metadata/gcs_metadata.json"
        |workspace-bucket-name-file-name = ".delocalize.json"
        |working-directory = "/tmp"
        |misc-http-client-config = {
        |  wsm-url = "https://workspace.dsde-dev.broadinstitute.org/"
        |  workspace-id = "a5a1f1e1-bcb0-49d9-b589-ea4d7c9d6f02"
        |  storage-container-resource-id = "9151f3a0-fe5c-49c5-b8a1-dc15cd96b174"
        |}
        |is-rstudio-runtime = false
        |object-service {
        |  working-directory = "/work"
        |  lock-expiration = 3 minutes
        |  owner-email = "fake@gmail.com"
        |}
        |
        |staging-bucket-name = "fakeBucket"
        |clean-up-lock-interval = 7 minutes
        |flush-cache-interval = 1 minutes
        |sync-cloud-storage-directory-interval = 15 minutes
        |delocalize-directory-interval = 30 seconds
        |is-rstudio-runtime = false
        |# value needs to be lower case
        |type = "gcp"
        |""".stripMargin

    val expectedConfig = AppConfig.Azure(
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
      false
    )
    ConfigSource.string(configString).load[AppConfig.Azure] shouldBe Right(expectedConfig)
  }

  it should "read AppConfig configuration correctly when it's gcp" ignore {
    val config = Config.appConfig
    val objectServiceConfig = ObjectServiceConfig(Paths.get("/work"), WorkbenchEmail("fake@gmail.com"), 3 minutes)

    val expectedConfig = AppConfig.Gcp(
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
      false
    )
    config shouldBe Right(expectedConfig)
  }

  it should "read AppConfig configuration correctly when it's azure" in {
    val config = Config.appConfig
    val objectServiceConfig = ObjectServiceConfig(Paths.get("/work"), WorkbenchEmail("fake@gmail.com"), 3 minutes)

    val expectedConfig = AppConfig.Azure(
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
      false
    )
    config shouldBe Right(expectedConfig)
  }
}
