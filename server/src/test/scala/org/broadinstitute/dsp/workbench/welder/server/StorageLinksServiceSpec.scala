package org.broadinstitute.dsp.workbench.welder.server

import java.nio.file.Path
import java.nio.file.Paths

import cats.effect.IO
import cats.effect.concurrent.Ref
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.http4s.Uri
import org.scalatest.{FlatSpec, Matchers}

class StorageLinksServiceSpec extends FlatSpec with Matchers{

  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]

  //TODO: remove boilerplate at the top of each test
  "StorageLinksService" should "create a storage link" in {
    val storageLinks = Ref.of[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(storageLinks.unsafeRunSync())

    val linkToAdd = StorageLink(Paths.get("/foo"), Uri.unsafeFromString("gs://foo/bar/baz.zip"), ".zip", true)

    val addResult = storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()
    assert(addResult equals linkToAdd)
  }

  it should "not create duplicate storage links" in {
    val storageLinks = Ref.of[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(storageLinks.unsafeRunSync())

    val linkToAdd = StorageLink(Paths.get("/foo"), Uri.unsafeFromString("gs://foo/bar/baz.zip"), ".zip", true)

    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()
    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    val listResult = storageLinksService.getStorageLinks.unsafeRunSync()

    assert(listResult.storageLinks equals Set(linkToAdd))
  }

  it should "list storage links" in {
    val storageLinks = Ref.of[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(storageLinks.unsafeRunSync())

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToAdd = StorageLink(Paths.get("/foo"), Uri.unsafeFromString("gs://foo/bar/baz.zip"), ".zip", true)

    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks equals Set(linkToAdd))
  }

  it should "delete a storage link" in {
    val storageLinks = Ref.of[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(storageLinks.unsafeRunSync())

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToAddAndRemove = StorageLink(Paths.get("/foo"), Uri.unsafeFromString("gs://foo/bar/baz.zip"), ".zip", true)

    storageLinksService.createStorageLink(linkToAddAndRemove).unsafeRunSync()

    val intermediateListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(intermediateListResult.storageLinks equals Set(linkToAddAndRemove))

    storageLinksService.deleteStorageLink(linkToAddAndRemove).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks.isEmpty)
  }

  it should "gracefully handle deleting a storage link that doesn't exist" in {
    val storageLinks = Ref.of[IO, Map[Path, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(storageLinks.unsafeRunSync())

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToRemove = StorageLink(Paths.get("/foo"), Uri.unsafeFromString("gs://foo/bar/baz.zip"), ".zip", true)

    storageLinksService.deleteStorageLink(linkToRemove).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks.isEmpty)
  }

}
