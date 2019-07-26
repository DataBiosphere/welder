package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.{Path, Paths}

import cats.effect.IO
import cats.effect.concurrent.Ref
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.RemoveObjectResult
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.scalatest.FlatSpec

import scala.util.matching.Regex

class StorageLinksServiceSpec extends FlatSpec with WelderTestSuite {
  val cloudStorageDirectory = CloudStorageDirectory(GcsBucketName("foo"), Some(BlobPath("bar/baz.zip")))
  val baseDir = LocalBaseDirectory(RelativePath(Paths.get("foo")))
  val baseSafeDir = LocalSafeBaseDirectory(RelativePath(Paths.get("bar")))

  val googleStorageAlg = new GoogleStorageAlg {
    override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] = IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
    override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = ???
    override def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] = ???
    override def gcsToLocalFile(localAbsolutePath: Path, gsPath: GsPath, traceId: TraceId): Stream[IO, AdaptedGcsMetadata] = ???
    override def delocalize(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, userDefinedMeta: Map[String, String], traceId: TraceId): IO[DelocalizeResponse] = ???
    override def localizeCloudDirectory(localBaseDirectory: RelativePath, cloudStorageDirectory: CloudStorageDirectory, workingDir: Path, pattern: Regex, traceId: TraceId): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
  }
  val emptyMetadataCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
  val metadataCacheAlg = new MetadataCacheInterp(emptyMetadataCache)
  val workingDirectory = Paths.get("/tmp")

  "StorageLinksService" should "create a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, workingDirectory)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    val addResult = storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()
    assert(addResult equals linkToAdd)
  }

  it should "not create duplicate storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, workingDirectory)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    val listResult = storageLinksService.getStorageLinks.unsafeRunSync()

    assert(listResult.storageLinks equals Set(linkToAdd))
  }

  it should "initialize directories" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, workingDirectory)

    val safeAbsolutePath = workingDirectory.resolve(baseSafeDir.path.asPath)
    val editAbsolutePath = workingDirectory.resolve(baseDir.path.asPath)

    val dirsToCreate: List[java.nio.file.Path] = List[java.nio.file.Path](safeAbsolutePath, editAbsolutePath)

    dirsToCreate
      .map(path => new java.io.File(path.toUri))
      .map(dir => if (dir.exists()) dir.delete())

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)
    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    dirsToCreate
      .map(path => assert(path.toFile.exists))
  }


//  initializeDirectories
  it should "list storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, workingDirectory)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    storageLinksService.createStorageLink(linkToAdd).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks equals Set(linkToAdd))
  }

  it should "delete a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, workingDirectory)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToAddAndRemove = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    storageLinksService.createStorageLink(linkToAddAndRemove).unsafeRunSync()

    val intermediateListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(intermediateListResult.storageLinks equals Set(linkToAddAndRemove))

    storageLinksService.deleteStorageLink(linkToAddAndRemove).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks.isEmpty)
  }

  it should "gracefully handle deleting a storage link that doesn't exist" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, workingDirectory)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToRemove = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    storageLinksService.deleteStorageLink(linkToRemove).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks.isEmpty)
  }
}
