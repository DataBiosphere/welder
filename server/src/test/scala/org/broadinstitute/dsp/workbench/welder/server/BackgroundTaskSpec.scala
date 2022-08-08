package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.{IO, Ref}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.LocalBaseDirectory
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.Paths
import scala.concurrent.duration._

class BackgroundTaskSpec extends AnyFlatSpec with WelderTestSuite {
  val backgroundTaskConfig = BackgroundTaskConfig(
    Paths.get("/work"),
    GcsBucketName("testStagingBucket"),
    7 minutes,
    10 minutes,
    15 minutes,
    30 seconds,
    true,
    WorkbenchEmail("test@email.com")
  )

  "getGsPath" should "return the correct path to delocalize files to" in {
    val storageLink =
      StorageLink(
        LocalBaseDirectory(RelativePath(Paths.get(""))),
        None,
        CloudStorageDirectory(GoogleCloudStorageContainer(GcsBucketName("testBucket")), Some(BlobPath("notebooks"))),
        "\\.Rmd$".r
      )
    val file = new File("test.Rmd")
    val res = initBackgroundTask(Map(storageLink.localBaseDirectory.path -> storageLink), Map.empty, None).getGsPath(storageLink, file)
    res.toString shouldBe s"gs://testBucket/notebooks/test.Rmd"
  }

  private def initBackgroundTask(
      storageLinks: Map[RelativePath, StorageLink],
      metadata: Map[RelativePath, AdaptedGcsMetadataCache],
      googleStorageService: Option[GoogleStorageService[IO]]
  ): BackgroundTask = {
    val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](storageLinks)
    val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](metadata)
    val defaultGoogleStorageAlg =
      CloudStorageAlg.forGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), googleStorageService.getOrElse(FakeGoogleStorageInterpreter))
    val metadataCacheAlg = new MetadataCacheInterp(metaCache)
    new BackgroundTask(
      backgroundTaskConfig,
      metaCache,
      storageLinksCache,
      Ref.unsafe[IO, CloudStorageAlg](defaultGoogleStorageAlg),
      metadataCacheAlg
    )
  }
}
