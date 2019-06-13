package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.{Path, Paths}

import cats.effect.IO
import cats.effect.concurrent.Ref
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.scalatest.{FlatSpec, Matchers}

class StorageLinksServiceSpec extends FlatSpec with Matchers {
  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]
  val cloudStorageDirectory = CloudStorageDirectory(GcsBucketName("foo"), BlobPath("bar/baz.zip"))
  val baseDir = LocalBaseDirectory(Paths.get("/foo"))
  val baseSafeDir = LocalSafeBaseDirectory(Paths.get("/bar"))

  //TODO: remove boilerplate at the top of each test
  "StorageLinksService" should "create a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    val addResult = storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()
    assert(addResult equals linkToAdd)
  }

  it should "not create duplicate storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    val listResult = storageLinksService.getStorageLinks.unsafeRunSync()

    assert(listResult.storageLinks equals Set(linkToAdd))
  }

  it should "list storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks equals Set(linkToAdd))
  }

  it should "delete a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToAddAndRemove = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    storageLinksService.createStorageLink(linkToAddAndRemove).unsafeRunSync()

    val intermediateListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(intermediateListResult.storageLinks equals Set(linkToAddAndRemove))

    storageLinksService.deleteStorageLink(linkToAddAndRemove).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks.isEmpty)
  }

  it should "gracefully handle deleting a storage link that doesn't exist" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToRemove = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    storageLinksService.deleteStorageLink(linkToRemove).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks.isEmpty)
  }
}
