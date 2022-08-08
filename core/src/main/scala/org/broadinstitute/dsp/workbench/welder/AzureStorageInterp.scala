package org.broadinstitute.dsp.workbench.welder
import cats.effect.IO
import cats.mtl.Ask
import fs2.Pipe
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.typelevel.log4cats.StructuredLogger

import java.nio.file.Path
import scala.util.matching.Regex

//TODO: Justin
class AzureStorageInterp(implicit logger: StructuredLogger[IO]) extends CloudStorageAlg {
  override def updateMetadata(gsPath: SourceUri.GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] = ???

  override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: SourceUri.GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = ???

  override def retrieveUserDefinedMetadata(gsPath: SourceUri.GsPath, traceId: TraceId): IO[Map[String, String]] = ???

  override def removeObject(gsPath: SourceUri.GsPath, traceId: TraceId, generation: Option[Long]): fs2.Stream[IO, RemoveObjectResult] = ???

  /**
    * Overwrites the file if it already exists locally
    */
  override def gcsToLocalFile(localAbsolutePath: Path, gsPath: SourceUri.GsPath, traceId: TraceId): fs2.Stream[IO, AdaptedGcsMetadata] = ???

  /**
    * Delocalize user's files to GCS.
    */
  override def delocalize(
      localObjectPath: RelativePath,
      gsPath: SourceUri.GsPath,
      generation: Long,
      userDefinedMeta: Map[String, String],
      traceId: TraceId
  ): IO[DelocalizeResponse] = ???

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  override def fileToGcs(localObjectPath: RelativePath, gsPath: SourceUri.GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = ???

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  override def fileToGcsAbsolutePath(localFile: Path, gsPath: SourceUri.GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = logger.info("TODO")

  /**
    * Recursively download files in cloudStorageDirectory to local directory.
    * If file exists locally, we don't download the file
    *
    * @param localBaseDirectory    : base directory where remote files will be download to
    * @param cloudStorageDirectory : GCS directory where files will be download from
    * @return AdaptedGcsMetadataCache that should be added to local metadata cache
    */
  override def localizeCloudDirectory(
      localBaseDirectory: RelativePath,
      cloudStorageDirectory: CloudStorageDirectory,
      workingDir: Path,
      pattern: Regex,
      traceId: TraceId
  ): fs2.Stream[IO, AdaptedGcsMetadataCache] = ???

  override def uploadBlob(bucketName: GcsBucketName, objectName: GcsBlobName): Pipe[IO, Byte, Unit] = ???

  override def getBlob[A: Decoder](bucketName: GcsBucketName, blobName: GcsBlobName): fs2.Stream[IO, A] = ???
}
