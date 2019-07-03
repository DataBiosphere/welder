package org.broadinstitute.dsp.workbench.welder

import java.nio.file.{Path, Paths}
import java.time.Instant

import cats.implicits._
import cats.effect.IO
import fs2.Stream
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.broadinstitute.dsp.workbench.welder.SourceUri.{DataUri, GsPath}
import org.http4s.Uri

object JsonCodec {
  implicit val pathDecoder: Decoder[Path] = Decoder.decodeString.emap(s => Either.catchNonFatal(Paths.get(s)).leftMap(_.getMessage))
  implicit val pathEncoder: Encoder[Path] = Encoder.encodeString.contramap(_.toString)
  implicit val relativePathDecoder: Decoder[RelativePath] = pathDecoder.emap { path =>
    if (path.isAbsolute) Left(s"${path} should be relative") else Right(RelativePath(path))
  }
  implicit val relativePathEncoder: Encoder[RelativePath] = pathEncoder.contramap(_.asPath)
  implicit val workbenchEmailEncoder: Encoder[WorkbenchEmail] = Encoder.encodeString.contramap(_.value)
  implicit val workbenchEmailDecoder: Decoder[WorkbenchEmail] = Decoder.decodeString.map(WorkbenchEmail)
  implicit val hashedMetadataEncoder: Encoder[HashedLockedBy] = Encoder.encodeString.contramap(_.asString)
  implicit val hashedMetadataDecoder: Decoder[HashedLockedBy] = Decoder.decodeString.map(HashedLockedBy)
  implicit val uriEncoder: Encoder[Uri] = Encoder.encodeString.contramap(_.renderString)
  implicit val uriDecoder: Decoder[Uri] = Decoder.decodeString.emap(s => Uri.fromString(s).leftMap(_.getMessage()))
  implicit val instanceEncoder: Encoder[Instant] = Encoder.encodeLong.contramap(_.toEpochMilli)
  implicit val syncStatusEncoder: Encoder[SyncStatus] = Encoder.encodeString.contramap(_.toString)
  implicit val gcsBucketNameDecoder: Decoder[GcsBucketName] = Decoder.decodeString.map(GcsBucketName)
  implicit val gcsBlobNameDecoder: Decoder[GcsBlobName] = Decoder.decodeString.map(GcsBlobName)
  implicit val gsPathDecoder: Decoder[GsPath] = Decoder.decodeString.emap(parseGsPath)
  implicit val gsPathEncoder: Encoder[GsPath] = Encoder.encodeString.contramap(_.toString)
  implicit val cloudStorageDirectoryDecoder: Decoder[CloudStorageDirectory] = Decoder.decodeString.emap { s =>
    parseBucketName(s) match {
      case Left(error) => Left(error)
      case Right(bucket) =>
        val length = s"gs://${bucket.value}/".length
        val blobPathString = s.drop(length)
        val blobPath = if(blobPathString.isEmpty) None else Some(BlobPath(blobPathString))
        Right(CloudStorageDirectory(bucket, blobPath))
    }
  }
  implicit val cloudStorageDirectoryEncoder: Encoder[CloudStorageDirectory] = Encoder.encodeString.contramap { x =>
    x.blobPath match {
      case Some(bp) => s"gs://${x.bucketName.value}/${bp.asString}"
      case None => s"gs://${x.bucketName.value}"
    }
  }
  implicit val sourceUriDecoder: Decoder[SourceUri] = Decoder.decodeString.emap { s =>
    if (s.startsWith("data:application/json;base64,")) {
      val res = for {
        encodedData <- Either.catchNonFatal(s.split(",")(1))
        data <- Either.catchNonFatal(Stream.emit(encodedData).through(base64DecoderPipe[IO]).compile.to[Array].unsafeRunSync())
      } yield DataUri(data)

      res.leftMap(_.getMessage)
    } else parseGsPath(s)
  }
  implicit val localBasePathEncoder: Encoder[LocalDirectory] = pathEncoder.contramap(_.path.asPath)

  implicit val storageLinkEncoder: Encoder[StorageLink] =
    Encoder.forProduct4("localBaseDirectory", "localSafeModeBaseDirectory", "cloudStorageDirectory", "pattern")(
      storageLink => StorageLink.unapply(storageLink).get
    )

  implicit val storageLinkDecoder: Decoder[StorageLink] = Decoder.instance { x =>
    for {
      baseDir <- x.downField("localBaseDirectory").as[Path]
      safeBaseDir <- x.downField("localSafeModeBaseDirectory").as[Path]
      cloudStorageDir <- x.downField("cloudStorageDirectory").as[CloudStorageDirectory]
      pattern <- x.downField("pattern").as[String]
    } yield StorageLink(LocalBaseDirectory(RelativePath(baseDir)), LocalSafeBaseDirectory(RelativePath(safeBaseDir)), cloudStorageDir, pattern)
  }

  implicit val syncModeEncoder: Encoder[SyncMode] = Encoder.encodeString.contramap(_.toString)
  implicit val crc32cEncoder: Encoder[Crc32] = Encoder.encodeString.contramap(_.asString)
  implicit val crc32cDecoder: Decoder[Crc32] = Decoder.decodeString.map(Crc32)
  implicit val lockEncoder: Encoder[Lock] = Encoder.forProduct2(
    "lastLockedBy",
    "lockExpiresAt"
  )(x => Lock.unapply(x).get)
  implicit val lockDecoder: Decoder[Lock] = Decoder.forProduct2(
    "lastLockedBy",
    "lockExpiresAt"
  )(Lock.apply)
  implicit val localFileStateInGCSEncoder: Encoder[LocalFileStateInGCS] = Encoder.forProduct2(
    "crc32c",
    "generation"
  )(x => LocalFileStateInGCS.unapply(x).get)
  implicit val localFileStateInGCSDecoder: Decoder[LocalFileStateInGCS] = Decoder.forProduct2(
    "crc32c",
    "generation"
  )(LocalFileStateInGCS.apply)
  implicit val gcsMetadataEncoder: Encoder[AdaptedGcsMetadataCache] = Encoder.forProduct3(
    "localPath",
    "lock",
    "localFileStateInGCS"
  )(x => AdaptedGcsMetadataCache.unapply(x).get)
  implicit val gcsMetadataDecoder: Decoder[AdaptedGcsMetadataCache] = Decoder.forProduct3(
    "localPath",
    "lock",
    "localFileStateInGCS"
  )(AdaptedGcsMetadataCache.apply)

  implicit val traceIdEncoder: Encoder[TraceId] = Encoder.encodeString.contramap(_.uuid.toString)
}
