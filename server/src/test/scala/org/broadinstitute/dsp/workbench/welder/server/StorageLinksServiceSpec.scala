package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.mtl.Ask
import fs2.Stream
import org.broadinstitute.dsde.workbench.util2.RemoveObjectResult
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.{Path, Paths}
import java.util.UUID
import scala.util.matching.Regex

class StorageLinksServiceSpec extends AnyFlatSpec with WelderTestSuite {
  val cloudStorageDirectory = CloudStorageDirectory(CloudStorageContainer("foo"), Some(BlobPath("bar/baz.zip")))
  val baseDir = LocalBaseDirectory(RelativePath(Paths.get("foo")))
  val baseSafeDir = LocalSafeBaseDirectory(RelativePath(Paths.get("bar")))

  val googleStorageAlgRef = Ref.unsafe[IO, CloudStorageAlg](new MockCloudStorageAlg {
    override def updateMetadata(gsPath: CloudBlobPath, metadata: Map[String, String])(implicit ev: Ask[IO, TraceId]): IO[UpdateMetadataResponse] =
      IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
    override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Option[AdaptedGcsMetadata]] = ???
    override def removeObject(gsPath: CloudBlobPath, generation: Option[Long])(implicit ev: Ask[IO, TraceId]): Stream[IO, RemoveObjectResult] = ???
    override def cloudToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: CloudBlobPath)(
        implicit ev: Ask[IO, TraceId]
    ): IO[Option[AdaptedGcsMetadata]] = ???
    override def delocalize(
        localObjectPath: RelativePath,
        gsPath: CloudBlobPath,
        generation: Long,
        userDefinedMeta: Map[String, String]
    )(implicit ev: Ask[IO, TraceId]): IO[Option[DelocalizeResponse]] = ???
    override def localizeCloudDirectory(
        localBaseDirectory: RelativePath,
        cloudStorageDirectory: CloudStorageDirectory,
        workingDir: Path,
        pattern: Regex
    )(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadataCache]] = Stream.empty
    override def fileToGcs(localObjectPath: RelativePath, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
    override def fileToGcsAbsolutePath(localFile: Path, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
  })
  val emptyMetadataCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
  val metadataCacheAlg = new MetadataCacheInterp(emptyMetadataCache)
  val config = StorageLinksServiceConfig(Paths.get("/tmp"), Paths.get("/tmp/WORKSPACE_BUCKET"))

  "StorageLinksService" should "create a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val res = Dispatcher[IO].use { d =>
      val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlgRef, metadataCacheAlg, config, d)

      val linkToAdd = StorageLink(baseDir, Some(baseSafeDir), cloudStorageDirectory, ".zip".r)

      for {
        addResult <- storageLinksService.createStorageLink(linkToAdd).run(TraceId(UUID.randomUUID().toString))
      } yield assert(addResult equals linkToAdd)
    }

    res.unsafeRunSync()
  }

  it should "not create duplicate storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val res = Dispatcher[IO].use { d =>
      val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlgRef, metadataCacheAlg, config, d)

      val linkToAdd = StorageLink(baseDir, Some(baseSafeDir), cloudStorageDirectory, ".zip".r)

      for {

        _ <- storageLinksService.createStorageLink(linkToAdd).run(TraceId(UUID.randomUUID().toString))
        listResult <- storageLinksService.getStorageLinks

      } yield assert(listResult.storageLinks equals Set(linkToAdd))
    }
    res.unsafeRunSync()
  }

  it should "initialize directories" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val res = Dispatcher[IO].use { d =>
      val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlgRef, metadataCacheAlg, config, d)

      val safeAbsolutePath = config.workingDirectory.resolve(baseSafeDir.path.asPath)
      val editAbsolutePath = config.workingDirectory.resolve(baseDir.path.asPath)
      val dirsToCreate: List[java.nio.file.Path] = List[java.nio.file.Path](safeAbsolutePath, editAbsolutePath)

      dirsToCreate
        .map(path => new java.io.File(path.toUri))
        .map(dir => if (dir.exists()) dir.delete())

      val linkToAdd = StorageLink(baseDir, Some(baseSafeDir), cloudStorageDirectory, ".zip".r)

      storageLinksService
        .createStorageLink(linkToAdd)
        .run(TraceId(UUID.randomUUID().toString))
        .map(_ =>
          dirsToCreate
            .map(path => assert(path.toFile.exists))
        )
    }

    res.unsafeRunSync()
  }

//  initializeDirectories
  it should "list storage links" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)

    val res = Dispatcher[IO].use { d =>
      val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlgRef, metadataCacheAlg, config, d)
      for {
        initialListResult <- storageLinksService.getStorageLinks
        _ = assert(initialListResult.storageLinks.isEmpty)
        linkToAdd = StorageLink(baseDir, Some(baseSafeDir), cloudStorageDirectory, ".zip".r)

        _ <- storageLinksService.createStorageLink(linkToAdd).run(TraceId(UUID.randomUUID().toString))
        finalListResult <- storageLinksService.getStorageLinks

      } yield assert(finalListResult.storageLinks equals Set(linkToAdd))
    }

    res.unsafeRunSync()
  }

  it should "delete a storage link" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val res = Dispatcher[IO].use { d =>
      val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlgRef, metadataCacheAlg, config, d)

      for {
        initialListResult <- storageLinksService.getStorageLinks
        _ = assert(initialListResult.storageLinks.isEmpty)
        linkToAddAndRemove = StorageLink(baseDir, Some(baseSafeDir), cloudStorageDirectory, ".zip".r)

        _ <- storageLinksService.createStorageLink(linkToAddAndRemove).run(TraceId(UUID.randomUUID().toString))
        intermediateListResult <- storageLinksService.getStorageLinks
        _ = assert(intermediateListResult.storageLinks equals Set(linkToAddAndRemove))

        _ <- storageLinksService.deleteStorageLink(linkToAddAndRemove)
        finalListResult <- storageLinksService.getStorageLinks

      } yield {
        assert(finalListResult.storageLinks.isEmpty)
        assert(finalListResult.storageLinks.isEmpty)
      }
    }

    res.unsafeRunSync()
  }

  it should "gracefully handle deleting a storage link that doesn't exist" in {
    val emptyStorageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)

    val res = Dispatcher[IO].use { d =>
      val storageLinksService = StorageLinksService(emptyStorageLinksCache, googleStorageAlgRef, metadataCacheAlg, config, d)

      for {
        initialListResult <- storageLinksService.getStorageLinks
        _ = assert(initialListResult.storageLinks.isEmpty)
        linkToRemove = StorageLink(baseDir, Some(baseSafeDir), cloudStorageDirectory, ".zip".r)

        _ <- storageLinksService.deleteStorageLink(linkToRemove)
        finalListResult <- storageLinksService.getStorageLinks

      } yield assert(finalListResult.storageLinks.isEmpty)
    }

    res.unsafeRunSync()
  }
}
