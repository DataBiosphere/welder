package org.broadinstitute.dsp.workbench.welder

import cats.effect.{Blocker, IO}
import cats.effect.concurrent.Ref
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.{Crc32, GetMetadataResponse, GoogleStorageService}
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.Generators.{genGsPath, genRmdStorageLink}
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Paths
import java.io.File
import scala.concurrent.duration._

class BackgroundTaskSpec extends AnyFlatSpec with WelderTestSuite {
  val backgroundTaskConfig = BackgroundTaskConfig(Paths.get("/work"), GcsBucketName("testStagingBucket"), 7 minutes, 10 minutes, 15 minutes, 30 seconds, true)

  "getGsPath" should "return the correct path to delocalize files to" in {
    val storageLink = genRmdStorageLink.sample.get
    val file = new File("test.Rmd")
    val res = initBackgroundTask(Map(storageLink.localBaseDirectory.path -> storageLink), Map.empty, None, blocker).getGsPath(storageLink, file)
    res.toString shouldBe s"gs://${storageLink.cloudStorageDirectory.bucketName.value}/${storageLink.cloudStorageDirectory.blobPath.get.asString}/test.Rmd"
  }

//  "shouldDelocalize" should "return true if files have changed" in {
//    val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 0L) //This crc32c is to not match the file's crc32c created in this test
//    val storageService = FakeGoogleStorageService(metadataResp)
//    val localAbsolutePath = Paths.get(s"/tmp/test.Rmd")
//    val bodyBytes = "this is great!".getBytes("UTF-8")
//    val storageLink = genRmdStorageLink.sample.get
//    val gsPath = genGsPath.sample.get
//    val backgroundTask = initBackgroundTask(Map(storageLink.localBaseDirectory.path -> storageLink), Map.empty, Some(storageService), blocker)
//    val res = for {
//      _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/test.Rmd"), blocker)).compile.drain
//      r <- backgroundTask.shouldDelocalize(gsPath, localAbsolutePath)
//    } yield (r shouldBe (true))
//    res.unsafeRunSync()
//  }

  "shouldDelocalize" should "return false if files have not changed" in {
    val metadataResp = GetMetadataResponse.Metadata(Crc32("trIjMQ=="), Map.empty, 0L) //This crc32c is from the file created in this test
    val storageService = FakeGoogleStorageService(metadataResp)
    val localAbsolutePath = Paths.get(s"/tmp/test.Rmd")
    val bodyBytes = "this is great!".getBytes("UTF-8")
    val storageLink = genRmdStorageLink.sample.get
    val gsPath = genGsPath.sample.get
    val backgroundTask = initBackgroundTask(Map(storageLink.localBaseDirectory.path -> storageLink), Map.empty, Some(storageService), blocker)
    val res = for {
      _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/test.Rmd"), blocker)).compile.drain
      r <- backgroundTask.shouldDelocalize(gsPath, localAbsolutePath)
    } yield (r shouldBe (false))
    res.unsafeRunSync()
  }

  "shouldDelocalize" should "return true if the file does not exist in Google" in {
    val metadataResp = GetMetadataResponse.NotFound
    val storageService = FakeGoogleStorageService(metadataResp)
    val localAbsolutePath = Paths.get(s"/tmp/test.Rmd")
    val bodyBytes = "this is great!".getBytes("UTF-8")
    val storageLink = genRmdStorageLink.sample.get
    val gsPath = genGsPath.sample.get
    val backgroundTask = initBackgroundTask(Map(storageLink.localBaseDirectory.path -> storageLink), Map.empty, Some(storageService), blocker)
    val res = for {
      _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/test.Rmd"), blocker)).compile.drain
      r <- backgroundTask.shouldDelocalize(gsPath, localAbsolutePath)
    } yield (r shouldBe (true))
    res.unsafeRunSync()
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
      googleStorageService.getOrElse(FakeGoogleStorageInterpreter),
      blocker
    )
  }
}
