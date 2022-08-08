package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.mtl.Ask
import fs2.{Pipe, Stream}
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName, GoogleStorageService, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.typelevel.log4cats.StructuredLogger
import java.nio.file.Path

import org.broadinstitute.dsde.workbench.azure.AzureStorageService

import scala.util.matching.Regex

trait CloudStorageAlg {
  def cloudProvider: CloudProvider

  abstract def updateMetadata(gsPath: SourceUri, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
    IO.raiseError[UpdateMetadataResponse](new RuntimeException(s"$cloudProvider storage interp updateMetadata should only be called with appropriate source URI"))

  def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: SourceUri, traceId: TraceId): IO[Option[AdaptedGcsMetadata]]
  def retrieveUserDefinedMetadata(gsPath: SourceUri, traceId: TraceId): IO[Map[String, String]]
  abstract def removeObject(gsPath: SourceUri, traceId: TraceId, generation: Option[Long])(implicit ev: Ask[IO, TraceId]): Stream[IO, RemoveObjectResult] =
    Stream.eval(IO.raiseError[RemoveObjectResult](new RuntimeException(s"$cloudProvider storage interp removeObject should only be called with appropriate source URI")))

  /**
    * Overwrites the file if it already exists locally
    */
  abstract def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: SourceUri, traceId: TraceId)(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadata]] =
    Stream.eval(IO.raiseError[Option[AdaptedGcsMetadata]](new RuntimeException(s"$cloudProvider storage interp remoteToLocalFile should only be called with appropriate source URI")))

  /**
    * Delocalize user's files to GCS.
    */
  abstract def delocalize(
      localObjectPath: RelativePath,
      gsPath: SourceUri,
      generation: Long,
      userDefinedMeta: Map[String, String],
      traceId: TraceId
  )(implicit ev: Ask[IO, TraceId]): IO[Option[DelocalizeResponse]] =
    IO.raiseError(new RuntimeException(s"$cloudProvider storage interp delocalize should only be called with appropriate source URI"))

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  abstract def fileToGcs(localObjectPath: RelativePath, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    IO.raiseError(new RuntimeException(s"$cloudProvider storage interp fileToGcs should only be called with appropriate source URI"))

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  abstract def fileToGcsAbsolutePath(localFile: Path, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    IO.raiseError(new RuntimeException(s"$cloudProvider storage interp fileToGcsAbsolutePath should only be called with appropriate source URI"))

  /**
    * Recursively download files in cloudStorageDirectory to local directory.
    * If file exists locally, we don't download the file
    * @param localBaseDirectory: base directory where remote files will be download to
    * @param cloudStorageDirectory: GCS directory where files will be download from
    * @return AdaptedGcsMetadataCache that should be added to local metadata cache
    */
  abstract  def localizeCloudDirectory(
      localBaseDirectory: RelativePath,
      cloudStorageDirectory: CloudStorageDirectory,
      workingDir: Path,
      pattern: Regex,
      traceId: TraceId
  )(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadataCache]] =
    Stream.eval(IO.raiseError(new RuntimeException(s"$cloudProvider storage interp localizeCloudDirectory should only be called with appropriate source URI")))

  abstract def uploadBlob(path: SourceUri)(implicit ev: Ask[IO, TraceId]): Pipe[IO, Byte, Unit] =
    _ => Stream.eval(IO.raiseError(new RuntimeException(s"$cloudProvider storage interp uploadBlob should only be called with appropriate source URI")))

  def getBlob[A: Decoder](path: SourceUri)(implicit ev: Ask[IO, TraceId]): Stream[IO, A]
}

object CloudStorageAlg {
  def forGoogle(
      config: GoogleStorageAlgConfig,
      googleStorageService: GoogleStorageService[IO]
  )(implicit logger: StructuredLogger[IO]): CloudStorageAlg =
    new GoogleStorageInterp(config, googleStorageService)

  def forAzure(config: GoogleStorageAlgConfig, azureStorageService: AzureStorageService[IO])(implicit logger: StructuredLogger[IO]): CloudStorageAlg =
    new AzureStorageInterp(config, azureStorageService)
}

//TODO: RENAME
final case class GoogleStorageAlgConfig(workingDirectory: Path)
final case class DelocalizeResponse(generation: Long, crc32c: Crc32)

sealed trait UpdateMetadataResponse extends Product with Serializable
object UpdateMetadataResponse {
  final case object DirectMetadataUpdate extends UpdateMetadataResponse
  final case class ReUploadObject(generation: Long, crc32c: Crc32) extends UpdateMetadataResponse
}
