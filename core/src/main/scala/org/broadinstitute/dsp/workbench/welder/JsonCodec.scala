package org.broadinstitute.dsp.workbench.welder

import java.nio.file.{Path, Paths}
import java.time.Instant
import cats.implicits._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import io.circe.{Decoder, Encoder}
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.broadinstitute.dsp.workbench.welder.SourceUri.{CloudUri, DataUri}
import org.http4s.Uri

import scala.util.matching.Regex

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
  implicit val gsPathDecoder: Decoder[CloudUri] = Decoder.decodeString.emap(parseCloudBlobPath).map(CloudUri)
  implicit val gsPathEncoder: Encoder[CloudUri] = Encoder.encodeString.contramap(_.toString)
  implicit val cloudStorageDirectoryDecoder: Decoder[CloudStorageDirectory] = Decoder.decodeString.emap { s =>
    val parsed = s.replaceAll("gs://", "")

    parseCloudStorageContainer(parsed) match {
      case Left(error) =>
        Left(error)
      case Right(bucket) =>
        val length = s"${bucket.name}/".length
        val blobPathString = parsed.drop(length)
        val blobPath = if (blobPathString.isEmpty) None else Some(BlobPath(blobPathString))
        Right(CloudStorageDirectory(CloudStorageContainer(bucket.name), blobPath))
    }
  }
  implicit val cloudStorageDirectoryEncoder: Encoder[CloudStorageDirectory] = Encoder.encodeString.contramap(x => x.asString)
  implicit val sourceUriDecoder: Decoder[SourceUri] = Decoder.decodeString.emap { s =>
    if (s.startsWith("data:application/json;base64,")) {
      val res = for {
        encodedData <- Either.catchNonFatal(s.split(",")(1))
        data <- Either.catchNonFatal(Stream.emit(encodedData).through(base64DecoderPipe[IO]).compile.to(Array).unsafeRunSync())
      } yield DataUri(data)

      res.leftMap(_.getMessage)
    } else parseCloudBlobPath(s).map(SourceUri.CloudUri)
  }
  implicit val localBasePathEncoder: Encoder[LocalDirectory] = pathEncoder.contramap(_.path.asPath)

  implicit val regexEncoder: Encoder[Regex] = Encoder.encodeString.contramap(_.pattern.pattern())

  implicit val regexDecoder: Decoder[Regex] = Decoder.decodeString.emap(s => Either.catchNonFatal(s.r).leftMap(_.getMessage))

  implicit val storageLinkEncoder: Encoder[StorageLink] =
    Encoder.forProduct4("localBaseDirectory", "localSafeModeBaseDirectory", "cloudStorageDirectory", "pattern")(storageLink =>
      StorageLink.unapply(storageLink).get
    )

  implicit val storageLinkDecoder: Decoder[StorageLink] = Decoder.instance { x =>
    for {
      baseDir <- x.downField("localBaseDirectory").as[Path]
      safeBaseDir <- x.downField("localSafeModeBaseDirectory").as[Option[Path]]
      cloudStorageDir <- x.downField("cloudStorageDirectory").as[CloudStorageDirectory]
      pattern <- x.downField("pattern").as[Regex]
    } yield StorageLink(LocalBaseDirectory(RelativePath(baseDir)), safeBaseDir.map(x => LocalSafeBaseDirectory(RelativePath(x))), cloudStorageDir, pattern)
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
  implicit val remoteStateExistEncoder: Encoder[RemoteState.Found] = Encoder.forProduct2(
    "lock",
    "crc32c"
  )(x => RemoteState.Found.unapply(x).get)
  implicit val remoteStateExistDecoder: Decoder[RemoteState.Found] = Decoder.forProduct2(
    "lock",
    "crc32c"
  )(RemoteState.Found.apply)
  implicit val remoteStateEncoder: Encoder[RemoteState] = Encoder.instance(x =>
    x match {
      case RemoteState.NotFound => "NotFound".asJson
      case e: RemoteState.Found => e.asJson
    }
  )
  implicit val remoteStateDecoder: Decoder[RemoteState] = Decoder.instance(x => x.as[RemoteState.Found].orElse(Right(RemoteState.NotFound)))
  implicit val gcsMetadataEncoder: Encoder[AdaptedGcsMetadataCache] = Encoder.forProduct3(
    "localPath",
    "remoteState",
    "localFileGeneration"
  )(x => AdaptedGcsMetadataCache.unapply(x).get)
  implicit val gcsMetadataDecoder: Decoder[AdaptedGcsMetadataCache] = Decoder.forProduct3(
    "localPath",
    "remoteState",
    "localFileGeneration"
  )(AdaptedGcsMetadataCache.apply)

  implicit val traceIdEncoder: Encoder[TraceId] = Encoder.encodeString.contramap(_.asString)
}
