package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.TimeUnit

import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.circe.Decoder
import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2.{Stream, io}
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName, GoogleStorageService, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.typelevel.jawn.AsyncParser

import scala.collection.JavaConverters._
import scala.util.matching.Regex

class GoogleStorageInterp(config: GoogleStorageAlgConfig, blocker: Blocker, googleStorageService: GoogleStorageService[IO])(
    implicit logger: Logger[IO],
    timer: Timer[IO],
    cs: ContextShift[IO]
) extends GoogleStorageAlg {
  def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
    googleStorageService
      .setObjectMetadata(gsPath.bucketName, gsPath.blobName, metadata, Option(traceId))
      .compile
      .drain
      .as(UpdateMetadataResponse.DirectMetadataUpdate)
      .handleErrorWith {
        case e: com.google.cloud.storage.StorageException if (e.getCode == 403) =>
          for {
            _ <- logger.info(s"$traceId | Fail to update lock due to 403. Going to download the blob and re-upload")
            bytes <- googleStorageService.getBlobBody(gsPath.bucketName, gsPath.blobName, Some(traceId)).compile.to(Array)
            blob <- googleStorageService.createBlob(gsPath.bucketName, gsPath.blobName, bytes, "text/plain", metadata, None, Some(traceId)).compile.lastOrError
          } yield UpdateMetadataResponse.ReUploadObject(blob.getGeneration, Crc32(blob.getCrc32c))
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
        .through(io.file.writeAll[IO](localAbsolutePath, blocker, writeFileOptions))) ++ Stream.eval(IO.unit)
      userDefinedMetadata = Option(blob.getMetadata).map(_.asScala.toMap).getOrElse(Map.empty)
      meta <- Stream.eval(adaptMetadata(Crc32(blob.getCrc32c), userDefinedMetadata, blob.getGeneration))
    } yield meta

  override def delocalize(
      localObjectPath: RelativePath,
      gsPath: GsPath,
      generation: Long,
      userDefinedMeta: Map[String, String],
      traceId: TraceId
  ): IO[DelocalizeResponse] = {
    val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
    io.file.readAll[IO](localAbsolutePath, blocker, 4096).compile.to(Array).flatMap { body =>
      googleStorageService
        .createBlob(gsPath.bucketName, gsPath.blobName, body, gcpObjectType, userDefinedMeta, Some(generation), Some(traceId))
        .map(x => DelocalizeResponse(x.getGeneration, Crc32(x.getCrc32c)))
        .compile
        .lastOrError
        .adaptError {
          case e: com.google.cloud.storage.StorageException if e.getCode == 412 =>
            GenerationMismatch(traceId, s"Remote version has changed for ${localAbsolutePath}. Generation mismatch (local generation: ${generation}). ${e.getMessage}")
        }
    }
  }

  override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = {
    val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
    fileToGcsAbsolutePath(localAbsolutePath, gsPath)
  }

  override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    logger.info(s"flushing file ${localFile}") >>
      io.file.readAll[IO](localFile, blocker, 4096).compile.to(Array).flatMap { body =>
        googleStorageService
          .createBlob(gsPath.bucketName, gsPath.blobName, body, gcpObjectType, Map.empty, None)
          .void
          .compile
          .lastOrError
      }

  override def localizeCloudDirectory(
      localBaseDirectory: RelativePath,
      cloudStorageDirectory: CloudStorageDirectory,
      workingDir: Path,
      pattern: Regex,
      traceId: TraceId
  ): Stream[IO, AdaptedGcsMetadataCache] = {
    val res = for {
      blob <- googleStorageService.listBlobsWithPrefix(
        cloudStorageDirectory.bucketName,
        cloudStorageDirectory.blobPath.map(_.asString).getOrElse(""),
        true,
        1000,
        Some(traceId)
      )
      r <- if (pattern.findFirstIn(blob.getName).isDefined) {
        for {
          localPath <- Stream.eval(IO.fromEither(getLocalPath(localBaseDirectory, cloudStorageDirectory.blobPath, blob.getName)))
          localAbsolutePath <- Stream.fromEither[IO](Either.catchNonFatal(workingDir.resolve(localPath.asPath)))
          result <- if (localAbsolutePath.toFile.exists()) Stream(None).covary[IO]
          else {
            val res = for {
              metadata <- adaptMetadata(Crc32(blob.getCrc32c), Option(blob.getMetadata).map(_.asScala.toMap).getOrElse(Map.empty), blob.getGeneration)
              _ <- mkdirIfNotExist(localAbsolutePath.getParent)
              _ <- IO(blob.downloadTo(localAbsolutePath))
            } yield Some(AdaptedGcsMetadataCache(localPath, RemoteState.Found(metadata.lock, metadata.crc32c), Some(metadata.generation)))
            Stream.eval(res)
          }
        } yield result
      } else Stream(None).covary[IO]
    } yield r
    res.unNone
  }

  def uploadBlob(bucketName: GcsBucketName, blobName: GcsBlobName, objectContents: Array[Byte]): Stream[IO, Unit] =
    googleStorageService.createBlob(bucketName, blobName, objectContents, gcpObjectType, Map.empty, None).void

  def getBlob[A: Decoder](bucketName: GcsBucketName, blobName: GcsBlobName): Stream[IO, A] =
    for {
      blob <- googleStorageService.getBlob(bucketName, blobName, None)
      a <- Stream
        .emits(blob.getContent())
        .through(fs2.text.utf8Decode)
        .through(_root_.io.circe.fs2.stringParser[IO](AsyncParser.SingleValue))
        .through(_root_.io.circe.fs2.decoder)
    } yield a

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
      lock = lastLockedBy.flatMap { hashedLockBy =>
        expiresAt.flatMap { ea =>
          if (currentTime < ea.toEpochMilli)
            Some(Lock(hashedLockBy, ea))
          else
            none[Lock] //we don't care who held lock if it has expired
        }
      }
    } yield {
      AdaptedGcsMetadata(lock, crc32c, generation)
    }
}
