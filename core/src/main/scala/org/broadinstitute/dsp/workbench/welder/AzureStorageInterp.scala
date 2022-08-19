package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.mtl.Ask
import cats.implicits._
import fs2.{Pipe, Stream, text}
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.model.TraceId
import org.typelevel.log4cats.StructuredLogger
import java.nio.file.Path

import com.azure.storage.blob.models.ListBlobsOptions
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.azure.{AzureStorageService, BlobName}
import org.broadinstitute.dsp.workbench.welder.SourceUri.AzurePath
import org.typelevel.jawn.AsyncParser
import org.broadinstitute.dsde.workbench.util2.RemoveObjectResult
import scala.util.matching.Regex

class AzureStorageInterp(config: StorageAlgConfig, azureStorageService: AzureStorageService[IO])(implicit logger: StructuredLogger[IO])
    extends CloudStorageAlg {
  override def cloudProvider: CloudProvider = CloudProvider.Azure

  override def updateMetadata(gsPath: SourceUri, metadata: Map[String, String])(implicit ev: Ask[IO, TraceId]): IO[UpdateMetadataResponse] = gsPath match {
    case _: AzurePath =>
      ev.ask[TraceId]
        .flatMap(traceId => IO.raiseError[UpdateMetadataResponse](NotImplementedException(traceId, "update metadata not implemented for azure storage")))
    case _ => super.updateMetadata(gsPath, metadata)
  }

  override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Option[AdaptedGcsMetadata]] =
    ev.ask[TraceId]
      .flatMap(traceId =>
        IO.raiseError[Option[AdaptedGcsMetadata]](NotImplementedException(traceId, "retrieveAdaptedGcsMetadata not implemented for azure storage"))
      )

  override def retrieveUserDefinedMetadata(gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Map[String, String]] =
    ev.ask[TraceId]
      .flatMap(traceId => IO.raiseError[Map[String, String]](NotImplementedException(traceId, "retrieveUserDefinedMetadata not implemented for azure storage")))

  override def removeObject(gsPath: SourceUri, generation: Option[Long])(implicit ev: Ask[IO, TraceId]): fs2.Stream[IO, RemoveObjectResult] =
    gsPath match {
      case AzurePath(containerName, blobName) =>
        for {
          //TODO: remove asInstanceOf with new wblibs version
          resp <- Stream.eval(azureStorageService.deleteBlob(containerName, blobName).map(_.asInstanceOf[RemoveObjectResult]))
        } yield resp
      case _ => super.removeObject(gsPath, generation)
    }

  /**
    * Overwrites the file if it already exists locally
    */
  override def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadata]] =
    gsPath match {
      case AzurePath(containerName, blobName) =>
        for {
          _ <- Stream
            .eval(azureStorageService.downloadBlob(containerName, blobName, localAbsolutePath, overwrite = true))
        } yield Option.empty[AdaptedGcsMetadata]
      case _ => super.gcsToLocalFile(localAbsolutePath, gsPath)
    }

  /**
    * Delocalize user's files to GCS.
    */
  override def delocalize(
      localObjectPath: RelativePath,
      gsPath: SourceUri,
      generation: Long,
      userDefinedMeta: Map[String, String]
  )(implicit ev: Ask[IO, TraceId]): IO[Option[DelocalizeResponse]] = gsPath match {
    case AzurePath(containerName, blobName) =>
      for {
        traceId <- ev.ask[TraceId]
        localAbsolutePath <- IO.pure(config.workingDirectory.resolve(localObjectPath.asPath))
        _ <- logger.info(Map(TRACE_ID_LOGGING_KEY -> traceId.asString))(s"Delocalizing file ${localAbsolutePath.toString}")
        fs2path = fs2.io.file.Path.fromNioPath(localAbsolutePath)
        _ <- (Files[IO].readAll(fs2path) through azureStorageService.uploadBlob(containerName, blobName, true)).compile.drain
      } yield Option.empty[DelocalizeResponse]
    case _ => super.delocalize(localObjectPath, gsPath, generation, userDefinedMeta)
  }

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  override def fileToGcs(localObjectPath: RelativePath, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] = {
    val localAbsolutePath = config.workingDirectory.resolve(localObjectPath.asPath)
    fileToGcsAbsolutePath(localAbsolutePath, gsPath)
  }

  /**
    * Copy file to GCS without checking generation, and adding user metadata
    */
  override def fileToGcsAbsolutePath(localFile: Path, gsPath: SourceUri)(implicit ev: Ask[IO, TraceId]): IO[Unit] = gsPath match {
    case AzurePath(containerName, blobName) =>
      val fs2Path = fs2.io.file.Path.fromNioPath(localFile)
      logger.info(s"flushing file ${localFile} to ${gsPath}") >>
        (Files[IO].readAll(fs2Path) through azureStorageService.uploadBlob(containerName, blobName, true)).compile.drain
    case _ => super.fileToGcsAbsolutePath(localFile, gsPath)
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
                  gcsToLocalFile(localAbsolutePath, AzurePath(cloudStorageDirectory.container.asAzureCloudContainer, BlobName(blobName)))
            }
          } yield result
        case None => Stream(None).covary[IO]
      }
    } yield r.flatMap(_ => Option.empty[AdaptedGcsMetadataCache])

  override def uploadBlob(path: SourceUri)(implicit ev: Ask[IO, TraceId]): Pipe[IO, Byte, Unit] = path match {
    case AzurePath(containerName, blobName) => azureStorageService.uploadBlob(containerName, blobName, true)
    case _ => super.uploadBlob(path)
  }

  override def getBlob[A: Decoder](path: SourceUri)(implicit ev: Ask[IO, TraceId]): fs2.Stream[IO, A] = path match {
    case AzurePath(containerName, blobName) =>
      for {
        blob <- azureStorageService
          .getBlob(containerName, blobName)
          .through(text.utf8.decode)
          .through(_root_.io.circe.fs2.stringParser[IO](AsyncParser.SingleValue))
          .through(_root_.io.circe.fs2.decoder)
      } yield blob
    case _ => super.getBlob(path)
  }

}
