package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.{Path, Paths}
import java.util.UUID

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.mtl.ApplicativeAsk
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.RemoveObjectResult
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.matching.Regex

class StorageLinksServiceSpec extends AnyFlatSpec with WelderTestSuite {
  val cloudStorageDirectory = CloudStorageDirectory(GcsBucketName("foo"), Some(BlobPath("bar/baz.zip")))
  val baseDir = LocalBaseDirectory(RelativePath(Paths.get("foo")))
  val baseSafeDir = LocalSafeBaseDirectory(RelativePath(Paths.get("bar")))

  val googleStorageAlg = new MockGoogleStorageAlg {
    override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
      IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
    override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = ???
    override def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] = ???
    override def gcsToLocalFile(localAbsolutePath: Path, gsPath: GsPath, traceId: TraceId): Stream[IO, AdaptedGcsMetadata] = ???
    override def delocalize(
        localObjectPath: RelativePath,
        gsPath: GsPath,
        generation: Long,
        userDefinedMeta: Map[String, String],
        traceId: TraceId
    ): IO[DelocalizeResponse] = ???
    override def localizeCloudDirectory(
        localBaseDirectory: RelativePath,
        cloudStorageDirectory: CloudStorageDirectory,
        workingDir: Path,
        pattern: Regex,
        traceId: TraceId
    ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
    override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit
    override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit
  }
  val emptyMetadataCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
  val metadataCacheAlg = new MetadataCacheInterp(emptyMetadataCache)
  val config = StorageLinksServiceConfig(Paths.get("/tmp"), Paths.get("/tmp/WORKSPACE_BUCKET"))

  "StorageLinksService" should "create a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, config, blocker)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    val addResult = storageLinksService.createStorageLink(linkToAdd).run(TraceId(UUID.randomUUID().toString)).unsafeRunSync()
    assert(addResult equals linkToAdd)
  }

  it should "not create duplicate storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, config, blocker)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    storageLinksService.createStorageLink(linkToAdd).run(TraceId(UUID.randomUUID().toString)).unsafeRunSync()

    val listResult = storageLinksService.getStorageLinks.unsafeRunSync()

    assert(listResult.storageLinks equals Set(linkToAdd))
  }

  it should "initialize directories" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, config, blocker)

    val safeAbsolutePath = config.workingDirectory.resolve(baseSafeDir.path.asPath)
    val editAbsolutePath = config.workingDirectory.resolve(baseDir.path.asPath)
    val dirsToCreate: List[java.nio.file.Path] = List[java.nio.file.Path](safeAbsolutePath, editAbsolutePath)

    dirsToCreate
      .map(path => new java.io.File(path.toUri))
      .map(dir => if (dir.exists()) dir.delete())

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)
    storageLinksService.createStorageLink(linkToAdd).run(TraceId(UUID.randomUUID().toString)).unsafeRunSync()

    dirsToCreate
      .map(path => assert(path.toFile.exists))
  }

//  initializeDirectories
  it should "list storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, config, blocker)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToAdd = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    storageLinksService.createStorageLink(linkToAdd).run(TraceId(UUID.randomUUID().toString)).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks equals Set(linkToAdd))
  }

  it should "delete a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, config, blocker)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToAddAndRemove = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    storageLinksService.createStorageLink(linkToAddAndRemove).run(TraceId(UUID.randomUUID().toString)).unsafeRunSync()

    val intermediateListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(intermediateListResult.storageLinks equals Set(linkToAddAndRemove))

    storageLinksService.deleteStorageLink(linkToAddAndRemove).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks.isEmpty)
  }

  it should "gracefully handle deleting a storage link that doesn't exist" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlg, metadataCacheAlg, config, blocker)

    val initialListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(initialListResult.storageLinks.isEmpty)

    val linkToRemove = StorageLink(baseDir, baseSafeDir, cloudStorageDirectory, ".zip".r)

    storageLinksService.deleteStorageLink(linkToRemove).unsafeRunSync()

    val finalListResult = storageLinksService.getStorageLinks.unsafeRunSync()
    assert(finalListResult.storageLinks.isEmpty)
  }
}
