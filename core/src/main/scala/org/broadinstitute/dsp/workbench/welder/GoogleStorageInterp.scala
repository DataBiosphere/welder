package org.broadinstitute.dsp.workbench.welder

import java.time.Instant
import java.util.concurrent.TimeUnit

import fs2.{Stream, io}
import cats.implicits._
import cats.effect.{ContextShift, IO, Timer}
import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.linebacker.Linebacker
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import GoogleStorageAlg._

class GoogleStorageInterp(config: GoogleStorageAlgConfig , googleStorageService: GoogleStorageService[IO])
                         (implicit logger: Logger[IO], timer: Timer[IO], linerBacker: Linebacker[IO], cs: ContextShift[IO]) extends GoogleStorageAlg {
  def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[Unit] = googleStorageService.setObjectMetadata(gsPath.bucketName, gsPath.blobName, metadata, Option(traceId)).compile.drain.handleErrorWith {
    case e: com.google.cloud.storage.StorageException if(e.getCode == 403) =>
      for {
        _ <- logger.info(s"$traceId | Fail to update lock due to 403. Going to download the blob and re-upload")
        bytes <- googleStorageService.getObjectBody(gsPath.bucketName, gsPath.blobName, Some(traceId)).compile.to[Array]
        _ <- googleStorageService.storeObject(gsPath.bucketName, gsPath.blobName, bytes, "text/plain", metadata, None, Some(traceId)).compile.drain
      } yield ()
  }

  def retrieveAdaptedGcsMetadata(localPath: RelativePath, bucketName: GcsBucketName, blobName: GcsBlobName, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = for {
    meta <- googleStorageService.getObjectMetadata(bucketName, blobName, Some(traceId)).compile.last
    res <- meta match {
      case Some(google2.GetMetadataResponse.Metadata(crc32c, userDefinedMetadata, generation)) =>
        for {
          lastLockedBy <- IO.pure(userDefinedMetadata.get(LAST_LOCKED_BY).map(WorkbenchEmail))
          expiresAt <- userDefinedMetadata.get(LOCK_EXPIRES_AT).flatTraverse {
            expiresAtString =>
              for {
                ea <- Either.catchNonFatal(expiresAtString.toLong).fold[IO[Option[Long]]](_ => logger.warn(s"Failed to convert $expiresAtString to epoch millis") *> IO.pure(None), l => IO.pure(Some(l)))
                instant = ea.map(Instant.ofEpochMilli)
              } yield instant
          }
          currentTime <- timer.clock.realTime(TimeUnit.MILLISECONDS)
          lastLock <- expiresAt.flatTraverse[IO, WorkbenchEmail] { ea =>
            if (currentTime > ea.toEpochMilli)
              IO.pure(none[WorkbenchEmail])
            else
              IO.pure(lastLockedBy)
          }
        } yield Some(AdaptedGcsMetadata(localPath, lastLock, crc32c, generation))
      case _ => IO.pure(None)
    }
  } yield res

  def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] = googleStorageService.removeObject(gsPath.bucketName, gsPath.blobName, generation, Some(traceId))

  def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gcsBucketName: GcsBucketName, gcsBlobName: GcsBlobName): Stream[IO, Unit] = {
    googleStorageService.getObjectBody(gcsBucketName, gcsBlobName, None) //get file from google.
      .through(io.file.writeAll(localAbsolutePath, linerBacker.blockingContext))
  }

  def delocalize(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, traceId: TraceId): IO[Unit] = {
    val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
    io.file.readAll[IO](localAbsolutePath, linerBacker.blockingContext, 4096).compile.to[Array].flatMap { body =>
      googleStorageService
        .storeObject(gsPath.bucketName, gsPath.blobName, body, gcpObjectType, Map.empty, Some(generation), Some(traceId))
        .compile
        .drain
        .adaptError {
          case e: com.google.cloud.storage.StorageException if e.getCode == 412 =>
            GenerationMismatch(s"Remote version has changed for ${localAbsolutePath}. Generation mismatch")
        }
    }
  }
}
