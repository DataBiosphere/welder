package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Paths

import cats.effect.IO
import cats.effect.concurrent.Ref
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StorageLinksInterpSpec extends AnyFlatSpec with ScalaCheckPropertyChecks with WelderTestSuite {
  "StorageLinksInterp" should "return fail if storagelink can't be found" in {
    forAll { (localFileDestination: RelativePath) =>
      val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
      val storageLinksAlg = new StorageLinksInterp(storageLinksCache)
      val res = storageLinksAlg.findStorageLink(localFileDestination)
      res.attempt.unsafeRunSync() shouldBe (Left(StorageLinkNotFoundException(fakeTraceId, s"No storage link found for ${localFileDestination.toString}")))
    }
  }

  it should "return isSafeMode true when localPath is in safe mode" in {
    forAll { (storageLink: StorageLink) =>
      val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(storageLink.localSafeModeBaseDirectory.get.path -> storageLink))
      val storageLinksAlg = new StorageLinksInterp(storageLinksCache)
      val localPath = RelativePath(Paths.get(s"${storageLink.localSafeModeBaseDirectory.get.path.toString}/test.ipynb"))
      val res = storageLinksAlg.findStorageLink(localPath)
      val expectedResult = CommonContext(true, storageLink.localSafeModeBaseDirectory.get.path, storageLink)
      res.unsafeRunSync() shouldBe (expectedResult)
    }
  }

  it should "return isSafeMode false when localPath is in edit mode" in {
    forAll { (storageLink: StorageLink) =>
      val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(storageLink.localBaseDirectory.path -> storageLink))
      val storageLinksAlg = new StorageLinksInterp(storageLinksCache)
      val localPath = RelativePath(Paths.get(s"${storageLink.localBaseDirectory.path.toString}/test.ipynb"))
      val res = storageLinksAlg.findStorageLink(localPath)
      val expectedResult = CommonContext(false, storageLink.localBaseDirectory.path, storageLink)
      res.unsafeRunSync() shouldBe (expectedResult)
    }
  }
}
