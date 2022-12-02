package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.mtl.Ask
import fs2.{Pipe, Stream}
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.{Crc32, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.typelevel.log4cats.StructuredLogger

import java.nio.file.Path
import org.broadinstitute.dsde.workbench.azure.AzureStorageService
import org.broadinstitute.dsde.workbench.util2.RemoveObjectResult

import scala.util.matching.Regex

trait CloudStorageAlg {
  def updateMetadata(gsPath: CloudBlobPath, metadata: Map[String, String])(implicit ev: Ask[IO, TraceId]): IO[UpdateMetadataResponse]

  def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Option[AdaptedGcsMetadata]]

  def retrieveUserDefinedMetadata(gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Map[String, String]]

  def removeObject(gsPath: CloudBlobPath, generation: Option[Long])(implicit ev: Ask[IO, TraceId]): Stream[IO, RemoveObjectResult]

  /**
    * Overwrites the file if it already exists locally
    */
  def cloudToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Option[AdaptedGcsMetadata]]

  /**
    * Delocalize user's files to GCS.
    */
  def delocalize(
      localObjectPath: RelativePath,
      gsPath: CloudBlobPath,
      generation: Long,
      userDefinedMeta: Map[String, String]
  )(implicit ev: Ask[IO, TraceId]): IO[Option[DelocalizeResponse]]

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  def fileToGcs(localObjectPath: RelativePath, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Unit]

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  def fileToGcsAbsolutePath(localFile: Path, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Unit]

  /**
    * Recursively download files in cloudStorageDirectory to local directory.
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

  def uploadBlob(path: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): Pipe[IO, Byte, Unit]

  def getBlob[A: Decoder](path: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): Stream[IO, A]
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
