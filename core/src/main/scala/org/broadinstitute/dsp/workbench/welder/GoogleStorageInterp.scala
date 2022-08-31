package org.broadinstitute.dsp.workbench.welder

import _root_.io.circe.Decoder
import _root_.org.typelevel.log4cats.StructuredLogger
import cats.effect.IO
import cats.implicits._
import cats.mtl.Ask
import fs2.io.file.Files
import fs2.{Pipe, Stream, text}
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{Crc32, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.util2.RemoveObjectResult
import org.typelevel.jawn.AsyncParser

import java.nio.file.Path
import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

class GoogleStorageInterp(config: StorageAlgConfig, googleStorageService: GoogleStorageService[IO])(implicit
    logger: StructuredLogger[IO]
) extends CloudStorageAlg {
  private val chunkSize = 1024 * 1024 * 2 // com.google.cloud.storage.BlobReadChannel.DEFAULT_CHUNK_SIZE

  override def updateMetadata(gsPath: CloudBlobPath, metadata: Map[String, String])(implicit ev: Ask[IO, TraceId]): IO[UpdateMetadataResponse] =
    ev.ask[TraceId].flatMap { traceId =>
      googleStorageService
        .setObjectMetadata(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs, metadata, Option(traceId))
        .compile
        .drain
        .as(UpdateMetadataResponse.DirectMetadataUpdate)
        .handleErrorWith {
          case e: com.google.cloud.storage.StorageException if e.getCode == 403 =>
            for {
              _ <- logger.warn(Map(TRACE_ID_LOGGING_KEY -> traceId.asString))(s"Fail to update lock due to 403. Going to download the blob and re-upload")
              bytes <- googleStorageService.getBlobBody(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs, Some(traceId)).compile.to(Array)
              _ <- (Stream
                .emits(bytes)
                .covary[IO] through googleStorageService
                .streamUploadBlob(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs, traceId = Some(traceId))).compile.drain
              blob <- googleStorageService.getBlob(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs, traceId = Some(traceId)).compile.lastOrError
            } yield UpdateMetadataResponse.ReUploadObject(blob.getGeneration, Crc32(blob.getCrc32c))
        }
    }

  override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Option[AdaptedGcsMetadata]] =
    for {
      traceId <- ev.ask[TraceId]
      meta <- googleStorageService.getObjectMetadata(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs, Some(traceId)).compile.last
      res <- meta match {
        case Some(google2.GetMetadataResponse.Metadata(crc32c, userDefinedMetadata, generation)) =>
          adaptMetadata(crc32c, userDefinedMetadata, generation).map(x => Some(x))
        case _ =>
          IO.pure(None)
      }
    } yield res

  override def retrieveUserDefinedMetadata(gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Map[String, String]] =
    for {
      traceId <- ev.ask[TraceId]
      meta <- googleStorageService.getObjectMetadata(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs, Some(traceId)).compile.last
      res <- meta match {
        case Some(google2.GetMetadataResponse.Metadata(_, userDefinedMetadata, _)) =>
          IO(userDefinedMetadata)
        case _ =>
          IO.pure(Map.empty[String, String])
      }
    } yield res

  override def removeObject(gsPath: CloudBlobPath, generation: Option[Long])(implicit ev: Ask[IO, TraceId]): Stream[IO, RemoveObjectResult] =
    for {
      traceId <- Stream.eval(ev.ask)
      r <- googleStorageService.removeObject(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs, generation, Some(traceId))
    } yield r

  override def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: CloudBlobPath)(implicit
      ev: Ask[IO, TraceId]
  ): Stream[IO, Option[AdaptedGcsMetadata]] =
    for {
      traceId <- Stream.eval(ev.ask)
      blob <- googleStorageService.getBlob(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs, None, Some(traceId))
      fs2Path = fs2.io.file.Path.fromNioPath(localAbsolutePath)
      _ <- (fs2.io
        .readInputStream[IO](
          IO(
            java.nio.channels.Channels
              .newInputStream {
                val reader = blob.reader()
                reader.setChunkSize(chunkSize)
                reader
              }
          ),
          chunkSize,
          closeAfterUse = true
        )
        .through(Files[IO].writeAll(fs2Path, writeFileOptions))) ++ Stream.eval(IO.unit)
      userDefinedMetadata = Option(blob.getMetadata).map(_.asScala.toMap).getOrElse(Map.empty)
      meta <- Stream.eval(adaptMetadata(Crc32(blob.getCrc32c), userDefinedMetadata, blob.getGeneration))
    } yield Some(meta)

  override def delocalize(
      localObjectPath: RelativePath,
      gsPath: CloudBlobPath,
      generation: Long,
      userDefinedMeta: Map[String, String]
  )(implicit ev: Ask[IO, TraceId]): IO[Option[DelocalizeResponse]] = {
    val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
    for {
      traceId <- ev.ask[TraceId]
      _ <- logger.info(Map(TRACE_ID_LOGGING_KEY -> traceId.asString))(s"Delocalizing file ${localAbsolutePath.toString}")
      fs2path = fs2.io.file.Path.fromNioPath(localAbsolutePath)
      _ <- (Files[IO].readAll(fs2path) through googleStorageService.streamUploadBlob(
        gsPath.container.asGcsBucket,
        gsPath.blobPath.asGcs,
        userDefinedMeta,
        Some(generation),
        true,
        Some(traceId)
      )).compile.drain.handleErrorWith {
        case e: com.google.cloud.storage.StorageException if e.getCode == 412 =>
          val msg = s"Remote version has changed for $localAbsolutePath. Generation mismatch (local generation: ${generation}). ${e.getMessage}"
          logger.info(Map(TRACE_ID_LOGGING_KEY -> traceId.asString), e)(msg) >> IO.raiseError(GenerationMismatch(traceId, msg))
        case e =>
          IO.raiseError(e)
      }
      blob <- googleStorageService.getBlob(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs, traceId = Some(traceId)).compile.lastOrError
    } yield Some(DelocalizeResponse(blob.getGeneration, Crc32(blob.getCrc32c)))
  }

  override def fileToGcs(localObjectPath: RelativePath, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = {
    val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
    fileToGcsAbsolutePath(localAbsolutePath, gsPath)
  }

  override def fileToGcsAbsolutePath(localFile: Path, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = {
    val fs2Path = fs2.io.file.Path.fromNioPath(localFile)
    logger.info(s"flushing file ${localFile}") >>
      (Files[IO].readAll(fs2Path) through googleStorageService.streamUploadBlob(gsPath.container.asGcsBucket, gsPath.blobPath.asGcs)).compile.drain
  }

  override def localizeCloudDirectory(
      localBaseDirectory: RelativePath,
      cloudStorageDirectory: CloudStorageDirectory,
      workingDir: Path,
      pattern: Regex
  )(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadataCache]] =
    for {
      traceId <- Stream.eval(ev.ask)
      blob <- googleStorageService.listBlobsWithPrefix(
        cloudStorageDirectory.container.asGcsBucket,
        cloudStorageDirectory.blobPath.map(_.asString).getOrElse(""),
        true,
        1000,
        Some(traceId)
      )
      r <-
        if (pattern.findFirstIn(blob.getName).isDefined) {
          for {
            localPath <- Stream.eval(IO.fromEither(getLocalPath(localBaseDirectory, cloudStorageDirectory.blobPath, blob.getName)))
            localAbsolutePath <- Stream.fromEither[IO](Either.catchNonFatal(workingDir.resolve(localPath.asPath)))
            result <-
              if (localAbsolutePath.toFile.exists()) Stream(None).covary[IO]
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

  override def uploadBlob(path: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): Pipe[IO, Byte, Unit] =
    googleStorageService.streamUploadBlob(path.container.asGcsBucket, path.blobPath.asGcs)

  override def getBlob[A: Decoder](path: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): Stream[IO, A] =
    for {
      blob <- googleStorageService.getBlob(path.container.asGcsBucket, path.blobPath.asGcs, None)
      a <- Stream
        .emits(blob.getContent())
        .through(text.utf8.decode)
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
      currentTime <- IO.realTimeInstant
      lock = lastLockedBy.flatMap { hashedLockBy =>
        expiresAt.flatMap { ea =>
          if (currentTime.toEpochMilli < ea.toEpochMilli)
            Some(Lock(hashedLockBy, ea))
          else
            none[Lock] //we don't care who held lock if it has expired
        }
      }
    } yield AdaptedGcsMetadata(lock, crc32c, generation)
}
