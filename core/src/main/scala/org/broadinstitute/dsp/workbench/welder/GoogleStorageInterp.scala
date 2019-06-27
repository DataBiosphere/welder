package org.broadinstitute.dsp.workbench.welder

import java.time.Instant
import java.util.concurrent.TimeUnit

import _root_.io.chrisdavenport.linebacker.Linebacker
import _root_.io.chrisdavenport.log4cats.Logger
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import fs2.{Stream, io}
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{Crc32, GoogleStorageService, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.workbench.welder.GoogleStorageAlg._
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import scala.collection.JavaConverters._

class GoogleStorageInterp(config: GoogleStorageAlgConfig, googleStorageService: GoogleStorageService[IO])(
    implicit logger: Logger[IO],
    timer: Timer[IO],
    linerBacker: Linebacker[IO],
    cs: ContextShift[IO]
) extends GoogleStorageAlg {
  def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[Unit] =
    googleStorageService.setObjectMetadata(gsPath.bucketName, gsPath.blobName, metadata, Option(traceId)).compile.drain.handleErrorWith {
      case e: com.google.cloud.storage.StorageException if (e.getCode == 403) =>
        for {
          _ <- logger.info(s"$traceId | Fail to update lock due to 403. Going to download the blob and re-upload")
          bytes <- googleStorageService.getBlobBody(gsPath.bucketName, gsPath.blobName, Some(traceId)).compile.to[Array]
          _ <- googleStorageService.createBlob(gsPath.bucketName, gsPath.blobName, bytes, "text/plain", metadata, None, Some(traceId)).compile.drain
        } yield ()
    }

  def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] =
    for {
      meta <- googleStorageService.getObjectMetadata(gsPath.bucketName, gsPath.blobName, Some(traceId)).compile.last
      res <- meta match {
        case Some(google2.GetMetadataResponse.Metadata(crc32c, userDefinedMetadata, generation)) =>
          adaptMetadata(crc32c, userDefinedMetadata, generation).map(x => Some(x))
        case _ =>
          IO.pure(None)
      }
    } yield res

  def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] =
    googleStorageService.removeObject(gsPath.bucketName, gsPath.blobName, generation, Some(traceId))

  def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: GsPath, traceId: TraceId): Stream[IO, AdaptedGcsMetadata] =
    for {
      blob <- googleStorageService.getBlob(gsPath.bucketName, gsPath.blobName, Some(traceId))
      _ <- (Stream
        .emits(blob.getContent())
        .covary[IO]
        .through(io.file.writeAll[IO](localAbsolutePath, linerBacker.blockingContext))) ++ Stream.eval(IO.unit)
      userDefinedMetadata = Option(blob.getMetadata).map(_.asScala.toMap).getOrElse(Map.empty)
      meta <- Stream.eval(adaptMetadata(Crc32(blob.getCrc32c), userDefinedMetadata, blob.getGeneration))
    } yield meta

  def delocalize(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, traceId: TraceId): IO[DelocalizeResponse] = {
    val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
    io.file.readAll[IO](localAbsolutePath, linerBacker.blockingContext, 4096).compile.to[Array].flatMap { body =>
      googleStorageService
        .createBlob(gsPath.bucketName, gsPath.blobName, body, gcpObjectType, Map.empty, Some(generation), Some(traceId))
        .map(x => DelocalizeResponse(x.getGeneration, Crc32(x.getCrc32c)))
        .compile
        .lastOrError
        .adaptError {
          case e: com.google.cloud.storage.StorageException if e.getCode == 412 =>
            GenerationMismatch(s"Remote version has changed for ${localAbsolutePath}. Generation mismatch")
        }
    }
  }

  private def adaptMetadata(crc32c: Crc32, userDefinedMetadata: Map[String, String], generation: Long): IO[AdaptedGcsMetadata] =
    for {
      lastLockedBy <- IO.pure(userDefinedMetadata.get(LAST_LOCKED_BY).map(HashedLockedBy))
      expiresAt <- userDefinedMetadata.get(LOCK_EXPIRES_AT).flatTraverse { expiresAtString =>
        for {
          ea <- Either
            .catchNonFatal(expiresAtString.toLong)
            .fold[IO[Option[Long]]](_ => logger.warn(s"Failed to convert $expiresAtString to epoch millis") *> IO.pure(None), l => IO.pure(Some(l)))
          instant = ea.map(Instant.ofEpochMilli)
        } yield instant
      }
      currentTime <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      lastLock <- expiresAt.flatTraverse[IO, HashedLockedBy] { ea =>
        if (currentTime > ea.toEpochMilli)
          IO.pure(none[HashedLockedBy])
        else
          IO.pure(lastLockedBy)
      }
    } yield {
      AdaptedGcsMetadata(lastLock, crc32c, generation)
    }
}
