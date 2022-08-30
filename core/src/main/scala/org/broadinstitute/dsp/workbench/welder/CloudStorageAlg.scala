package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.mtl.Ask
import fs2.{Pipe, Stream}
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.{Crc32, GoogleStorageService, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.typelevel.log4cats.StructuredLogger
import java.nio.file.Path

import org.broadinstitute.dsde.workbench.azure.AzureStorageService

import scala.util.matching.Regex

trait CloudStorageAlg {
  def cloudProvider: CloudProvider

  def updateMetadata(gsPath: SourceUri, metadata: Map[String, String])(implicit ev: Ask[IO, TraceId]): IO[UpdateMetadataResponse] =
    makeInvalidSourceUriException("retrieveUserDefinedMetadata", gsPath).flatMap(e => IO.raiseError(e))

  def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Option[AdaptedGcsMetadata]] =
    makeInvalidSourceUriException("retrieveAdaptedGcsMetadata", gsPath).flatMap(e => IO.raiseError(e))

  def retrieveUserDefinedMetadata(gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Map[String, String]] =
    makeInvalidSourceUriException("retrieveUserDefinedMetadata", gsPath).flatMap(e => IO.raiseError(e))

  def removeObject(gsPath: SourceUri, generation: Option[Long])(implicit ev: Ask[IO, TraceId]): Stream[IO, RemoveObjectResult] =
    Stream.eval(makeInvalidSourceUriException("removeObject", gsPath).flatMap(e => IO.raiseError(e)))

  /** Overwrites the file if it already exists locally
    */
  def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadata]] =
    Stream.eval(makeInvalidSourceUriException("gcsToLocalFile", gsPath).flatMap(e => IO.raiseError(e)))

  /** Delocalize user's files to GCS.
    */
  def delocalize(
      localObjectPath: RelativePath,
      gsPath: SourceUri,
      generation: Long,
      userDefinedMeta: Map[String, String]
  )(implicit ev: Ask[IO, TraceId]): IO[Option[DelocalizeResponse]] =
    makeInvalidSourceUriException("delocalize", gsPath).flatMap(e => IO.raiseError(e))

  /** Copy file to GCS without checking generation, and adding user metadata
    */
  def fileToGcs(localObjectPath: RelativePath, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    makeInvalidSourceUriException("fileToGcs", gsPath).flatMap(e => IO.raiseError(e))

  /** Copy file to GCS without checking generation, and adding user metadata
    */
  def fileToGcsAbsolutePath(localFile: Path, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    makeInvalidSourceUriException("fileToGcsAbsolutePath", gsPath).flatMap(e => IO.raiseError(e))

  /** Recursively download files in cloudStorageDirectory to local directory.
    * If file exists locally, we don't download the file
    * @param localBaseDirectory: base directory where remote files will be download to
    * @param cloudStorageDirectory: GCS directory where files will be download from
    * @return AdaptedGcsMetadataCache that should be added to local metadata cache
    */
  def localizeCloudDirectory(
      localBaseDirectory: RelativePath,
      cloudStorageDirectory: CloudStorageDirectory,
      workingDir: Path,
      pattern: Regex
  )(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadataCache]]

  def uploadBlob(path: SourceUri)(implicit ev: Ask[IO, TraceId]): Pipe[IO, Byte, Unit] =
    _ => Stream.eval(makeInvalidSourceUriException("uploadBlob", path).flatMap(e => IO.raiseError(e)))

  def getBlob[A: Decoder](path: SourceUri)(implicit ev: Ask[IO, TraceId]): Stream[IO, A] =
    Stream.eval(makeInvalidSourceUriException("getBlob", path).flatMap(e => IO.raiseError(e)))

  private def makeInvalidSourceUriException(functionName: String, sourceUri: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[InvalidSourceURIException] =
    ev.ask[TraceId].map { traceId =>
      InvalidSourceURIException(
        traceId,
        s"storageInterp function $functionName was called for a source URI from an invalid cloud provider. \n\tExpected cloud provider: $cloudProvider. \n\tSourceUri: $sourceUri"
      )
    }

}

object CloudStorageAlg {
  def forGoogle(
      config: StorageAlgConfig,
      googleStorageService: GoogleStorageService[IO]
  )(implicit logger: StructuredLogger[IO]): CloudStorageAlg =
    new GoogleStorageInterp(config, googleStorageService)

  def forAzure(config: StorageAlgConfig, azureStorageService: AzureStorageService[IO])(implicit logger: StructuredLogger[IO]): CloudStorageAlg =
    new AzureStorageInterp(config, azureStorageService)
}

final case class StorageAlgConfig(workingDirectory: Path)
final case class DelocalizeResponse(generation: Long, crc32c: Crc32)

sealed trait UpdateMetadataResponse extends Product with Serializable
object UpdateMetadataResponse {
  final case object DirectMetadataUpdate extends UpdateMetadataResponse
  final case class ReUploadObject(generation: Long, crc32c: Crc32) extends UpdateMetadataResponse
}
