package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.implicits._
import cats.mtl.Ask
import com.azure.storage.blob.models.ListBlobsOptions
import fs2.io.file.Files
import fs2.{Pipe, Stream, text}
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.azure.AzureStorageService
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.util2.RemoveObjectResult
import org.typelevel.jawn.AsyncParser
import org.typelevel.log4cats.StructuredLogger

import java.nio.file.Path
import scala.util.matching.Regex

class AzureStorageInterp(config: StorageAlgConfig, azureStorageService: AzureStorageService[IO])(implicit logger: StructuredLogger[IO])
    extends CloudStorageAlg {

  override def updateMetadata(gsPath: CloudBlobPath, metadata: Map[String, String])(implicit ev: Ask[IO, TraceId]): IO[UpdateMetadataResponse] =
    ev.ask[TraceId]
      .flatMap(traceId => IO.raiseError[UpdateMetadataResponse](NotImplementedException(traceId, "update metadata not implemented for azure storage")))

  override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Option[AdaptedGcsMetadata]] =
    ev.ask[TraceId]
      .flatMap(traceId =>
        IO.raiseError[Option[AdaptedGcsMetadata]](NotImplementedException(traceId, "retrieveAdaptedGcsMetadata not implemented for azure storage"))
      )

  override def retrieveUserDefinedMetadata(gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Map[String, String]] =
    ev.ask[TraceId]
      .flatMap(traceId => IO.raiseError[Map[String, String]](NotImplementedException(traceId, "retrieveUserDefinedMetadata not implemented for azure storage")))

  override def removeObject(gsPath: CloudBlobPath, generation: Option[Long])(implicit ev: Ask[IO, TraceId]): fs2.Stream[IO, RemoveObjectResult] =
    for {
      resp <- Stream.eval(azureStorageService.deleteBlob(gsPath.container.asAzureCloudContainer, gsPath.blobPath.asAzure))
    } yield resp

  /**
    * Overwrites the file if it already exists locally
    */
  override def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: CloudBlobPath)(
      implicit ev: Ask[IO, TraceId]
  ): Stream[IO, Option[AdaptedGcsMetadata]] =
    for {
      _ <- Stream
        .eval(azureStorageService.downloadBlob(gsPath.container.asAzureCloudContainer, gsPath.blobPath.asAzure, localAbsolutePath, overwrite = true))
    } yield Option.empty[AdaptedGcsMetadata]

  /**
    * Delocalize user's files to GCS.
    */
  override def delocalize(
      localObjectPath: RelativePath,
      gsPath: CloudBlobPath,
      generation: Long,
      userDefinedMeta: Map[String, String]
  )(implicit ev: Ask[IO, TraceId]): IO[Option[DelocalizeResponse]] =
    for {
      traceId <- ev.ask[TraceId]
      localAbsolutePath <- IO.pure(config.workingDirectory.resolve(localObjectPath.asPath))
      _ <- logger.info(Map(TRACE_ID_LOGGING_KEY -> traceId.asString))(s"Delocalizing file ${localAbsolutePath.toString}")
      fs2path = fs2.io.file.Path.fromNioPath(localAbsolutePath)
      _ <- (Files[IO].readAll(fs2path) through azureStorageService.uploadBlob(gsPath.container.asAzureCloudContainer, gsPath.blobPath.asAzure, true)).compile.drain
    } yield none[DelocalizeResponse]

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  override def fileToGcs(localObjectPath: RelativePath, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = {
    val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
    fileToGcsAbsolutePath(localAbsolutePath, gsPath)
  }

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  override def fileToGcsAbsolutePath(localFile: Path, gsPath: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = {
    val fs2Path = fs2.io.file.Path.fromNioPath(localFile)
    logger.info(s"flushing file ${localFile} to ${gsPath}") >>
      (Files[IO].readAll(fs2Path) through azureStorageService.uploadBlob(gsPath.container.asAzureCloudContainer, gsPath.blobPath.asAzure, true)).compile.drain
  }

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
      pattern: Regex
  )(implicit ev: Ask[IO, TraceId]): fs2.Stream[IO, Option[AdaptedGcsMetadataCache]] =
    for {
      blob <- azureStorageService.listObjects(
        cloudStorageDirectory.container.asAzureCloudContainer,
        Some(
          new ListBlobsOptions()
            .setMaxResultsPerPage(1000)
            .setPrefix(cloudStorageDirectory.blobPath.map(_.asString).getOrElse(""))
        )
      )
      r <- pattern.findFirstIn(blob.getName) match {
        case Some(blobName) =>
          for {
            localPath <- Stream.eval(IO.fromEither(getLocalPath(localBaseDirectory, cloudStorageDirectory.blobPath, blob.getName)))
            localAbsolutePath <- Stream.fromEither[IO](Either.catchNonFatal(workingDir.resolve(localPath.asPath)))

            result <- localAbsolutePath.toFile.exists() match {
              case true => Stream(None).covary[IO]
              case false =>
                Stream.eval(mkdirIfNotExist(localAbsolutePath.getParent)) >>
                  gcsToLocalFile(localAbsolutePath, CloudBlobPath(cloudStorageDirectory.container, CloudStorageBlob(blobName)))
            }
          } yield result
        case None => Stream(None).covary[IO]
      }
    } yield r.flatMap(_ => Option.empty[AdaptedGcsMetadataCache])

  override def uploadBlob(path: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): Pipe[IO, Byte, Unit] =
    azureStorageService.uploadBlob(path.container.asAzureCloudContainer, path.blobPath.asAzure, true)

  override def getBlob[A: Decoder](path: CloudBlobPath)(implicit ev: Ask[IO, TraceId]): fs2.Stream[IO, A] =
    for {
      blob <- azureStorageService
        .getBlob(path.container.asAzureCloudContainer, path.blobPath.asAzure)
        .through(text.utf8.decode)
        .through(_root_.io.circe.fs2.stringParser[IO](AsyncParser.SingleValue))
        .through(_root_.io.circe.fs2.decoder)
    } yield blob

}
