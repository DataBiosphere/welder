package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.implicits._
import cats.mtl.Ask
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.typelevel.log4cats.StructuredLogger

import java.io.File
import java.nio.file.Path
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

class BackgroundTask(
    config: BackgroundTaskConfig,
    metadataCache: MetadataCache,
    storageLinksCache: StorageLinksCache,
    googleStorageAlg: GoogleStorageAlg,
    metadataCacheAlg: MetadataCacheAlg
)(implicit logger: StructuredLogger[IO]) {

  val cleanUpLock: Stream[IO, Unit] = {
    val task = (for {
      now <- IO.realTimeInstant
      updatedMap <- metadataCache.modify { mp =>
        val newMap = mp.map { kv =>
          kv._2.remoteState match {
            case RemoteState.NotFound => kv
            case RemoteState.Found(lock, crc32c) =>
              val newLock = lock.filter(l => l.lockExpiresAt.toEpochMilli < now.toEpochMilli)
              (kv._1 -> kv._2.copy(remoteState = RemoteState.Found(newLock, crc32c))) //This can be a bit cleaner with monocle
          }
        }
        (newMap, newMap)
      }
      _ <- logger.info(s"updated metadata cache size ${updatedMap.size}")
      _ <- logger.debug(s"updated metadata cache ${updatedMap}")
    } yield ()).handleErrorWith(t => logger.error(t)("fail to update metadata cache"))

    (Stream.sleep[IO](config.cleanUpLockInterval) ++ Stream.eval(task)).repeat
  }

  def flushBothCache(
      storageLinksJsonBlobName: GcsBlobName,
      gcsMetadataJsonBlobName: GcsBlobName
  )(implicit logger: StructuredLogger[IO]): Stream[IO, Unit] =
    (Stream.sleep[IO](config.flushCacheInterval) ++ Stream.eval(flushBothCacheOnce(storageLinksJsonBlobName, gcsMetadataJsonBlobName))).repeat

  def flushBothCacheOnce(
      storageLinksJsonBlobName: GcsBlobName,
      gcsMetadataJsonBlobName: GcsBlobName
  )(implicit logger: StructuredLogger[IO]): IO[Unit] = {
    val flushStorageLinks = flushCache(googleStorageAlg, config.stagingBucket, storageLinksJsonBlobName, storageLinksCache).handleErrorWith { t =>
      logger.info(t)("failed to flush storagelinks cache to GCS")
    }
    val flushMetadataCache = flushCache(googleStorageAlg, config.stagingBucket, gcsMetadataJsonBlobName, metadataCache).handleErrorWith { t =>
      logger.info(t)("failed to flush metadata cache to GCS")
    }
    List(flushStorageLinks, flushMetadataCache).parSequence_
  }

  val syncCloudStorageDirectory: Stream[IO, Unit] = {
    val res = for {
      storageLinks <- storageLinksCache.get
      traceId <- IO(TraceId(UUID.randomUUID().toString))
      _ <- storageLinks.values.toList.traverse { storageLink =>
        logger.info(s"syncing file from ${storageLink.cloudStorageDirectory}") >>
          (googleStorageAlg
            .localizeCloudDirectory(
              storageLink.localBaseDirectory.path,
              storageLink.cloudStorageDirectory,
              config.workingDirectory,
              storageLink.pattern,
              traceId
            )
            .through(metadataCacheAlg.updateCachePipe))
            .compile
            .drain
      }
    } yield ()

    (Stream.sleep[IO](config.syncCloudStorageDirectoryInterval) ++ Stream.eval(res)).repeat
  }

  val delocalizeBackgroundProcess: Stream[IO, Unit] = {
    if (config.isRstudioRuntime) {
      val res = (for {
        storageLinks <- storageLinksCache.get
        implicit0(tid: Ask[IO, TraceId]) <- IO(TraceId(UUID.randomUUID().toString)).map(tid => Ask.const[IO, TraceId](tid))
        _ <- storageLinks.values.toList.traverse { storageLink =>
          findFilesWithPattern(config.workingDirectory.resolve(storageLink.localBaseDirectory.path.asPath), storageLink.pattern).traverse_ { file =>
            val gsPath = getGsPath(storageLink, new File(file.getName))
            checkSyncStatus(
              gsPath,
              RelativePath(java.nio.file.Paths.get(file.getName))
            )
          }
        }
      } yield ()).handleErrorWith(r => logger.info(r)(s"Unexpected error encountered ${r}"))
      (Stream.sleep[IO](config.delocalizeDirectoryInterval) ++ Stream.eval(res)).repeat
    } else {
      Stream.eval(logger.info("Not running rmd sync process because this is not an Rstudio runtime"))
    }
  }

  def checkSyncStatus(gsPath: GsPath, localObjectPath: RelativePath)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    for {
      traceId <- ev.ask[TraceId]
      hashedOwnerEmail <- IO.fromEither(hashString(lockedByString(gsPath.bucketName, config.ownerEmail)))
      bucketMetadata <- googleStorageAlg.retrieveUserDefinedMetadata(gsPath, traceId)
      _ <- bucketMetadata match {
        case meta if meta.nonEmpty => {
          if (meta.get(hashedOwnerEmail.asString).contains("doNotSync")) {
            logger.info(s"skipping file ${localObjectPath.toString} doNotSync!")
          } else {
            checkCacheBeforeDelocalizing(gsPath, localObjectPath, hashedOwnerEmail, bucketMetadata)
          }
        }
        case _ => checkCacheBeforeDelocalizing(gsPath, localObjectPath, hashedOwnerEmail, bucketMetadata)
      }
    } yield ()

  private def checkCacheBeforeDelocalizing(
      gsPath: GsPath,
      localObjectPath: RelativePath,
      hashedOwnerEmail: HashedLockedBy,
      existingMetadata: Map[String, String]
  )(
      implicit ev: Ask[IO, TraceId]
  ): IO[Unit] =
    for {
      traceId <- ev.ask[TraceId]
      localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
      previousMeta <- metadataCacheAlg.getCache(localObjectPath)
      calculatedCrc32c <- Crc32c.calculateCrc32ForFile(localAbsolutePath)

      _ <- previousMeta match {
        case Some(meta) =>
          meta.remoteState match {
            case RemoteState.NotFound =>
              delocalizeAndUpdateCache(localObjectPath, gsPath, 0L, traceId, hashedOwnerEmail, existingMetadata)
            case RemoteState.Found(_, crc32c) =>
              if (calculatedCrc32c == crc32c)
                IO.unit
              else
                delocalizeAndUpdateCache(localObjectPath, gsPath, meta.localFileGeneration.getOrElse(0L), traceId, hashedOwnerEmail, existingMetadata)
          }
        case None =>
          delocalizeAndUpdateCache(localObjectPath, gsPath, 0L, traceId, hashedOwnerEmail, existingMetadata)
      }
    } yield ()

  private def delocalizeAndUpdateCache(
      localObjectPath: RelativePath,
      gsPath: GsPath,
      generation: Long,
      traceId: TraceId,
      hashedOwnerEmail: HashedLockedBy,
      existingMetadata: Map[String, String]
  ): IO[Unit] = {
    val updatedMetadata = existingMetadata + ("lastModifiedBy" -> hashedOwnerEmail.asString)
    for {
      delocalizeResp <- googleStorageAlg
        .delocalize(localObjectPath, gsPath, generation, updatedMetadata, traceId)
        .recoverWith {
          case e: GenerationMismatch =>
            if (generation == 0L)
              IO.raiseError(e)
            else {
              // In the case when the file is already been deleted from GCS, we try to delocalize the file with generation being 0L
              // This assumes the business logic we want is always to recreate files that have been deleted from GCS by other users.
              // If the file is indeed out of sync with remote, both delocalize attempts will fail due to generation mismatch
              googleStorageAlg.delocalize(
                localObjectPath,
                gsPath,
                0L,
                updatedMetadata,
                traceId
              )
            }
        }
        .recoverWith {
          case e: GenerationMismatch =>
            googleStorageAlg.updateMetadata(gsPath, traceId, Map(hashedOwnerEmail.asString -> "outdated")) >> IO.raiseError[DelocalizeResponse](e)
        }
      _ <- metadataCacheAlg.updateLocalFileStateCache(localObjectPath, RemoteState.Found(None, delocalizeResp.crc32c), delocalizeResp.generation)
    } yield ()
  }

  def getGsPath(storageLink: StorageLink, file: File): GsPath = {
    val fullBlobPath = getFullBlobName(
      storageLink.localBaseDirectory.path,
      storageLink.localBaseDirectory.path.asPath.resolve(file.toString),
      storageLink.cloudStorageDirectory.blobPath
    )
    GsPath(storageLink.cloudStorageDirectory.bucketName, fullBlobPath)
  }
}

final case class BackgroundTaskConfig(
    workingDirectory: Path,
    stagingBucket: GcsBucketName,
    cleanUpLockInterval: FiniteDuration,
    flushCacheInterval: FiniteDuration,
    syncCloudStorageDirectoryInterval: FiniteDuration,
    delocalizeDirectoryInterval: FiniteDuration,
    isRstudioRuntime: Boolean,
    ownerEmail: WorkbenchEmail
)
