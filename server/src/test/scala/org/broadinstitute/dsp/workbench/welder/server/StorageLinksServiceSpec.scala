package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Paths

import cats.effect.IO
import cats.effect.concurrent.Ref
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.scalatest.{FlatSpec, Matchers}

class StorageLinksServiceSpec extends FlatSpec with Matchers {
  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]
  val cloudStorageDirectory = CloudStorageDirectory(GcsBucketName("foo"), Some(BlobPath("bar/baz.zip")))
  val baseDir = LocalBaseDirectory(RelativePath(Paths.get("foo")))
  val baseSafeDir = LocalSafeBaseDirectory(RelativePath(Paths.get("bar")))

  val workingDirectory = Paths.get("/tmp")

  //TODO: remove boilerplate at the top of each test
  "StorageLinksService" should "create a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, workingDirectory)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    val addResult = storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()
    assert(addResult equals linkToAdd)
  }

  it should "not create duplicate storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, workingDirectory)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    val listResult = storageLinksService.getStorageLinks.unsafeRunSync()

    assert(listResult.storageLinks equals Set(linkToAdd))
  }

  it should "initialize directories" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, workingDirectory)

    val safeAbsolutePath = workingDirectory.resolve(baseSafeDir.path.asPath)
    val editAbsolutePath = workingDirectory.resolve(baseDir.path.asPath)

    val dirsToCreate: List[java.nio.file.Path] = List[java.nio.file.Path](safeAbsolutePath, editAbsolutePath)

    dirsToCreate
      .map(path => new java.io.File(path.toUri))
      .map(dir => if (dir.exists()) dir.delete())

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")
    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    dirsToCreate
      .map(path => new java.io.File(path.toUri))
      .map(dir => assert(dir.exists))
  }


//  initializeDirectories
  it should "list storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, workingDirectory)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks equals Set(linkToAdd))
  }

  it should "delete a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, workingDirectory)

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
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, workingDirectory)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToRemove = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip")

    storageLinksService.deleteStorageLink(linkToRemove).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks.isEmpty)
  }
}
