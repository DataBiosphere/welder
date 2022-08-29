package org.broadinstitute.dsp.workbench.welder

import _root_.io.circe.Decoder
import _root_.org.typelevel.log4cats.StructuredLogger
import cats.effect.IO
import cats.implicits._
import cats.mtl.Ask
import fs2.io.file.Files
import fs2.{Pipe, Stream, text}
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{Crc32, GoogleStorageService, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.typelevel.jawn.AsyncParser

import java.nio.file.Path
import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

class GoogleStorageInterp(config: StorageAlgConfig, googleStorageService: GoogleStorageService[IO])(
    implicit logger: StructuredLogger[IO]
) extends CloudStorageAlg {
  private val chunkSize = 1024 * 1024 * 2 // com.google.cloud.storage.BlobReadChannel.DEFAULT_CHUNK_SIZE

  override def updateMetadata(gsPath: SourceUri, metadata: Map[String, String])(implicit ev: Ask[IO, TraceId]): IO[UpdateMetadataResponse] = gsPath match {
    case GsPath(bucketName, blobName) =>
      ev.ask[TraceId].flatMap { traceId =>
        googleStorageService
          .setObjectMetadata(bucketName, blobName, metadata, Option(traceId))
          .compile
          .drain
          .as(UpdateMetadataResponse.DirectMetadataUpdate)
          .handleErrorWith {
            case e: com.google.cloud.storage.StorageException if (e.getCode == 403) =>
              for {
                _ <- logger.warn(Map(TRACE_ID_LOGGING_KEY -> traceId.asString))(s"Fail to update lock due to 403. Going to download the blob and re-upload")
                bytes <- googleStorageService.getBlobBody(bucketName, blobName, Some(traceId)).compile.to(Array)
                _ <- (Stream
                  .emits(bytes)
                  .covary[IO] through googleStorageService.streamUploadBlob(bucketName, blobName, traceId = Some(traceId))).compile.drain
                blob <- googleStorageService.getBlob(bucketName, blobName, traceId = Some(traceId)).compile.lastOrError
              } yield UpdateMetadataResponse.ReUploadObject(blob.getGeneration, Crc32(blob.getCrc32c))
          }
      }
    case _ => super.updateMetadata(gsPath, metadata)
  }

  override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Option[AdaptedGcsMetadata]] =
    gsPath match {
      case GsPath(bucketName, blobName) =>
        for {
          traceId <- ev.ask[TraceId]
          meta <- googleStorageService.getObjectMetadata(bucketName, blobName, Some(traceId)).compile.last
          res <- meta match {
            case Some(google2.GetMetadataResponse.Metadata(crc32c, userDefinedMetadata, generation)) =>
              adaptMetadata(crc32c, userDefinedMetadata, generation).map(x => Some(x))
            case _ =>
              IO.pure(None)
          }
        } yield res
      case _ => super.retrieveAdaptedGcsMetadata(localPath, gsPath)
    }

  override def retrieveUserDefinedMetadata(gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Map[String, String]] = gsPath match {
    case GsPath(bucketName, blobName) =>
      for {
        traceId <- ev.ask[TraceId]
        meta <- googleStorageService.getObjectMetadata(bucketName, blobName, Some(traceId)).compile.last
        res <- meta match {
          case Some(google2.GetMetadataResponse.Metadata(_, userDefinedMetadata, _)) =>
            IO(userDefinedMetadata)
          case _ =>
            IO.pure(Map.empty[String, String])
        }
      } yield res
    case _ => super.retrieveUserDefinedMetadata(gsPath)
  }

  override def removeObject(gsPath: SourceUri, generation: Option[Long])(implicit ev: Ask[IO, TraceId]): Stream[IO, RemoveObjectResult] = gsPath match {
    case GsPath(bucketName, blobName) =>
      for {
        traceId <- Stream.eval(ev.ask)
        r <- googleStorageService.removeObject(bucketName, blobName, generation, Some(traceId))
      } yield r
    case _ => super.removeObject(gsPath, generation)
  }

  override def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadata]] =
    gsPath match {
      case GsPath(bucketName, blobName) =>
        for {
          traceId <- Stream.eval(ev.ask)
          blob <- googleStorageService.getBlob(bucketName, blobName, None, Some(traceId))
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
      case _ => super.gcsToLocalFile(localAbsolutePath, gsPath)
    }

  override def delocalize(
      localObjectPath: RelativePath,
      gsPath: SourceUri,
      generation: Long,
      userDefinedMeta: Map[String, String]
  )(implicit ev: Ask[IO, TraceId]): IO[Option[DelocalizeResponse]] = gsPath match {
    case GsPath(bucketName, blobName) =>
      val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
      for {
        traceId <- ev.ask[TraceId]
        _ <- logger.info(Map(TRACE_ID_LOGGING_KEY -> traceId.asString))(s"Delocalizing file ${localAbsolutePath.toString}")
        fs2path = fs2.io.file.Path.fromNioPath(localAbsolutePath)
        _ <- (Files[IO].readAll(fs2path) through googleStorageService.streamUploadBlob(
          bucketName,
          blobName,
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
        blob <- googleStorageService.getBlob(bucketName, blobName, traceId = Some(traceId)).compile.lastOrError
      } yield Some(DelocalizeResponse(blob.getGeneration, Crc32(blob.getCrc32c)))
    case _ => super.delocalize(localObjectPath, gsPath, generation, userDefinedMeta)
  }

  override def fileToGcs(localObjectPath: RelativePath, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] = {
    val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
    fileToGcsAbsolutePath(localAbsolutePath, gsPath)
  }

  override def fileToGcsAbsolutePath(localFile: Path, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] = gsPath match {
    case GsPath(bucketName, blobName) =>
      val fs2Path = fs2.io.file.Path.fromNioPath(localFile)
      logger.info(s"flushing file ${localFile}") >>
        (Files[IO].readAll(fs2Path) through googleStorageService.streamUploadBlob(bucketName, blobName)).compile.drain
    case _ => super.fileToGcsAbsolutePath(localFile, gsPath)
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

  override def uploadBlob(path: SourceUri)(implicit ev: Ask[IO, TraceId]): Pipe[IO, Byte, Unit] = path match {
    case GsPath(bucketName, blobName) => googleStorageService.streamUploadBlob(bucketName, blobName)
    case _ => super.uploadBlob(path)
  }

  override def getBlob[A: Decoder](path: SourceUri)(implicit ev: Ask[IO, TraceId]): Stream[IO, A] = path match {
    case GsPath(bucketName, blobName) =>
      for {
        blob <- googleStorageService.getBlob(bucketName, blobName, None)
        a <- Stream
          .emits(blob.getContent())
          .through(text.utf8.decode)
          .through(_root_.io.circe.fs2.stringParser[IO](AsyncParser.SingleValue))
          .through(_root_.io.circe.fs2.decoder)
      } yield a
    case _ => super.getBlob(path)
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
      currentTime <- IO.realTimeInstant
      lock = lastLockedBy.flatMap { hashedLockBy =>
        expiresAt.flatMap { ea =>
          if (currentTime.toEpochMilli < ea.toEpochMilli)
            Some(Lock(hashedLockBy, ea))
          else
            none[Lock] //we don't care who held lock if it has expired
        }
      }
    } yield {
      AdaptedGcsMetadata(lock, crc32c, generation)
    }

  override def cloudProvider: CloudProvider = CloudProvider.Gcp
}
