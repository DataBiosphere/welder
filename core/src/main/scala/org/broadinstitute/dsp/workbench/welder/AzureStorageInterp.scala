package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.mtl.Ask
import cats.implicits._
import fs2.Pipe
import fs2.Stream
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.RemoveObjectResult
import org.broadinstitute.dsde.workbench.model.TraceId
import org.typelevel.log4cats.StructuredLogger
import java.nio.file.Path

import com.azure.storage.blob.models.ListBlobsOptions
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.azure.{AzureStorageService, BlobName, ContainerName}
import org.broadinstitute.dsp.workbench.welder.SourceUri.{AzurePath, GsPath}

import scala.util.matching.Regex

class AzureStorageInterp(config: GoogleStorageAlgConfig, azureStorageService: AzureStorageService[IO])(implicit logger: StructuredLogger[IO]) extends CloudStorageAlg {
  override def cloudProvider: CloudProvider = CloudProvider.Azure

  override def updateMetadata(gsPath: SourceUri, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] = gsPath match {
    case _:AzurePath => IO.raiseError[UpdateMetadataResponse](NotImplementedException(traceId, "update metadata not implemented for azure storage"))
    case _ => super.updateMetadata(gsPath, traceId, metadata)
  }

  override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: SourceUri, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = IO.raiseError[Option[AdaptedGcsMetadata]](NotImplementedException(traceId, "retrieveAdaptedGcsMetadata not implemented for azure storage"))

  override def retrieveUserDefinedMetadata(gsPath: SourceUri, traceId: TraceId): IO[Map[String, String]] = IO.raiseError[Map[String, String]](NotImplementedException(traceId, "retrieveUserDefinedMetadata not implemented for azure storage"))

  override def removeObject(gsPath: SourceUri, traceId: TraceId, generation: Option[Long])(implicit ev: Ask[IO, TraceId]): fs2.Stream[IO, RemoveObjectResult] =
    gsPath match {
      case AzurePath(containerName, blobName) =>
        for {
          //TODO: remove asInstanceOf with new wblibs version
          resp <- Stream.eval(azureStorageService.deleteBlob(containerName, blobName).map(_.asInstanceOf[RemoveObjectResult]))
        } yield resp
      case _ => super.removeObject(gsPath, traceId, generation)
    }

  /**
    * Overwrites the file if it already exists locally
    */
  override def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gsPath: SourceUri, traceId: TraceId)(implicit ev: Ask[IO, TraceId]): Stream[IO, Option[AdaptedGcsMetadata]] = gsPath match {
    case AzurePath(containerName, blobName) =>
      for {
        _ <- Stream
          .eval(azureStorageService.downloadBlob(containerName, blobName, localAbsolutePath, overwrite = true))
      } yield Option.empty[AdaptedGcsMetadata]
    case _ => super.gcsToLocalFile(localAbsolutePath, gsPath, traceId)
  }

  /**
    * Delocalize user's files to GCS.
    */
  override def delocalize(
      localObjectPath: RelativePath,
      gsPath: SourceUri,
      generation: Long,
      userDefinedMeta: Map[String, String],
      traceId: TraceId
  )(implicit ev: Ask[IO, TraceId]): IO[Option[DelocalizeResponse]] = gsPath match {
    case AzurePath(containerName, blobName) =>
      for {
        localAbsolutePath <- IO.pure(config.workingDirectory.resolve(localObjectPath.asPath))
        _ <- logger.info(Map(TRACE_ID_LOGGING_KEY -> traceId.asString))(s"Delocalizing file ${localAbsolutePath.toString}")
        fs2path = fs2.io.file.Path.fromNioPath(localAbsolutePath)
        _ <- (Files[IO].readAll(fs2path) through azureStorageService.uploadBlob(containerName, blobName))
            .compile
            .drain
      } yield Option.empty[DelocalizeResponse]
    case _ => super.delocalize(localObjectPath, gsPath, generation, userDefinedMeta, traceId)
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
      logger.info(s"flushing file ${localFile}") >>
        (Files[IO].readAll(fs2Path) through azureStorageService.uploadBlob(containerName, blobName)).compile.drain
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
      pattern: Regex,
      traceId: TraceId
  )(implicit ev: Ask[IO, TraceId]): fs2.Stream[IO, Option[AdaptedGcsMetadataCache]] =
    for {
      traceId <- Stream.eval(ev.ask)
      azureContainer = cloudStorageDirectory.containerName.asAzureCloudContainer
      blob <- azureStorageService.listObjects(azureContainer,
        Some(new ListBlobsOptions()
          .setMaxResultsPerPage(1000)
          .setPrefix(
            cloudStorageDirectory
              .blobPath.map(_.asString).getOrElse(""))
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
                  gcsToLocalFile(localAbsolutePath, AzurePath(azureContainer, BlobName(blobName)), traceId)
            }

//              localAbsolutePath.toFile.exists() match {
//              case false => Stream(None).covary[IO]
//              case true =>
//                val downloadBlob = Stream.eval(mkdirIfNotExist(localAbsolutePath.getParent)) >> gcsToLocalFile(localAbsolutePath, AzurePath(azureContainer, BlobName(blobName)), traceId)
//            }
          } yield result
        case None => Stream(None).covary[IO]
      }
    } yield r


  override def uploadBlob(path: SourceUri)(implicit ev: Ask[IO, TraceId]): Pipe[IO, Byte, Unit] = path match {
    case AzurePath(containerName, blobName) => azureStorageService.uploadBlob(containerName, blobName)
    case _ => super.uploadBlob(path)
  }

  override def getBlob[A: Decoder](path: SourceUri)(implicit ev: Ask[IO, TraceId]): fs2.Stream[IO, A] = Stream.eval(
    ev.ask.flatMap(
      traceId => IO.raiseError[A](NotImplementedException(traceId, "getBlob not implemented for azure storage interp"))
    ))

}
