package org.broadinstitute.dsp.workbench.welder

import cats.effect.{Blocker, IO}
import cats.effect.concurrent.Ref
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.LocalBaseDirectory
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Paths
import java.io.File
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
        CloudStorageDirectory(GcsBucketName("testBucket"), Some(BlobPath("notebooks"))),
        "\\.Rmd$".r
      )
    val file = new File("test.Rmd")
    val res = initBackgroundTask(Map(storageLink.localBaseDirectory.path -> storageLink), Map.empty, None, blocker).getGsPath(storageLink, file)
    res.toString shouldBe s"gs://testBucket/notebooks/test.Rmd"
  }

  private def initBackgroundTask(
      storageLinks: Map[RelativePath, StorageLink],
      metadata: Map[RelativePath, AdaptedGcsMetadataCache],
      googleStorageService: Option[GoogleStorageService[IO]],
      blocker: Blocker
  ): BackgroundTask = {
    val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](storageLinks)
    val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](metadata)
    val defaultGoogleStorageAlg =
      GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), blocker, googleStorageService.getOrElse(FakeGoogleStorageInterpreter))
    val metadataCacheAlg = new MetadataCacheInterp(metaCache)
    new BackgroundTask(
      backgroundTaskConfig,
      metaCache,
      storageLinksCache,
      defaultGoogleStorageAlg,
      metadataCacheAlg,
      blocker
    )
  }
}
