package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path
import java.time.Instant
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

import _root_.fs2.{Stream, io}
import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.circe.{Decoder, Encoder}
import _root_.io.circe.syntax._
import ca.mrvisser.sealerate
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.LocalBasePath.{LocalBaseDirectoryPath, LocalSafeBaseDirectoryPath}
import org.broadinstitute.dsp.workbench.welder.SourceUri.{DataUri, GsPath}
import org.broadinstitute.dsp.workbench.welder.server.ObjectService._
import org.broadinstitute.dsp.workbench.welder.server.PostObjectRequest._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class ObjectService(
    config: ObjectServiceConfig,
    googleStorageService: GoogleStorageService[IO],
    blockingEc: ExecutionContext,
    storageLinksCache: StorageLinksCache,
    metadataCache: MetadataCache
)(implicit cs: ContextShift[IO], logger: Logger[IO], timer: Timer[IO])
    extends Http4sDsl[IO] {
  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ GET -> Root / "metadata" =>
      for {
        metadataReq <- req.as[GetMetadataRequest]
        res <- checkMetadata(metadataReq)
        resp <- Ok(res)
      } yield resp
    case req @ POST -> Root =>
      for {
        localizeReq <- req.as[PostObjectRequest]
        res <- localizeReq match {
          case x: Localize => localize(x)
          case x: SafeDelocalize => safeDelocalize(x)
        }
        resp <- Ok(res)
      } yield resp
  }

  def localize(req: Localize): IO[Unit] = {
    val res = Stream
      .emits(req.entries)
      .map { entry =>
        entry.sourceUri match {
          case DataUri(data) => Stream.emits(data).through(io.file.writeAll[IO](entry.localObjectPath, blockingEc))
          case GsPath(bucketName, blobName) =>
            for {
              traceId <- Stream.emit(TraceId(randomUUID())).covary[IO]
              _ <- googleStorageService.getObject(bucketName, blobName, None) //get file from google.
                      .through(io.file.writeAll(entry.localObjectPath, blockingEc)) //write file to local disk
              // update metadata cache
              meta <- Stream.eval(retrieveGcsMetadata(bucketName, blobName, traceId))
              _ <- meta match {
                    case Some(m) =>
                      Stream.eval_(metadataCache.modify(mp => (mp + (entry.localObjectPath -> m), ())))
                    case _ => Stream.eval(IO.unit)
                  }
            } yield ()
        }
      }
      .parJoin(10)

    res.compile.drain
  }

  private def retrieveGcsMetadata(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: TraceId): IO[Option[GcsMetadata]] = for {
    meta <- googleStorageService.getObjectMetadata(bucketName, blobName, Some(traceId)).compile.last
    res <- meta match {
      case Some(google2.GetMetadataResponse.Metadata(crc32c, userDefinedMetadata, generation)) =>
        for {
          lastLockedBy <- IO.pure(userDefinedMetadata.get("lastLockedBy").map(WorkbenchEmail))
          expiresAt <- userDefinedMetadata.get("expiresAt").flatTraverse {
            expiresAtString =>
              for {
                ea <- Either.catchNonFatal(expiresAtString.toLong).fold[IO[Option[Long]]](_ => logger.warn(s"Failed to convert $expiresAtString to epoch millis") *> IO.pure(None), l => IO.pure(Some(l)))
                instant = ea.map(Instant.ofEpochMilli)
              } yield instant
          }
        } yield Some(GcsMetadata(lastLockedBy, expiresAt, crc32c, generation))
      case _ => IO.pure(None)
    }
  } yield res

  def checkMetadata(req: GetMetadataRequest): IO[MetadataResponse] =
    for {
      traceId <- IO(TraceId(randomUUID()))
      storageLinks <- storageLinksCache.get
      baseDirectory <- IO.fromEither(getLocalBaseDirectory(req.localObjectPath).leftMap(s => BadRequestException(s)))
      storageLink = storageLinks.find(x => x._1.path == baseDirectory)
      res <- retrieveMetadata(storageLink, traceId, req.localObjectPath)
    } yield res

  private def retrieveMetadata(storageLink: Option[(LocalBasePath, StorageLink)], traceId: TraceId, localPath: java.nio.file.Path): IO[MetadataResponse] =
    storageLink.fold[IO[MetadataResponse]](IO.raiseError(StorageLinkNotFoundException(s"No storage link found for ${localPath}"))) { pair =>
      val isSafeMode = pair._1 match {
        case LocalBaseDirectoryPath(_) => false
        case LocalSafeBaseDirectoryPath(_) => true
      }
      val sl = pair._2
      for {
        fullBlobName <- IO.fromEither(getFullBlobName(localPath, sl.cloudStorageDirectory.blobPath).leftMap(s => BadRequestException(s)))
        metadata <- retrieveGcsMetadata(sl.cloudStorageDirectory.bucketName, fullBlobName, traceId)
        result <- metadata match {
          case None =>
            IO(MetadataResponse.RemoteNotFound(isSafeMode, sl))
          case Some(GcsMetadata(lastLockedBy, expiresAt, crc32c, generation)) =>
            val localAbsolutePath = config.workingDirectory.resolve(localPath)
            for {
              fileBody <- io.file.readAll[IO](localAbsolutePath, blockingEc, 4096).compile.to[Array]
              calculatedCrc32c = Crc32c.calculateCrc32c(fileBody)
              syncStatus = if (calculatedCrc32c == crc32c) SyncStatus.Live else SyncStatus.Desynchronized
              currentTime <- timer.clock.realTime(TimeUnit.MILLISECONDS)
              lastLock <- expiresAt.flatTraverse[IO, WorkbenchEmail] { ea =>
                if (currentTime > ea.toEpochMilli)
                  IO.pure(none[WorkbenchEmail])
                else
                  IO.pure(lastLockedBy)
              }
            } yield MetadataResponse.EditMode(syncStatus, lastLock, isSafeMode, generation, sl)
        }
      } yield result
    }

  def safeDelocalize(req: SafeDelocalize): IO[Unit] =
    for {
      traceId <- IO(TraceId(randomUUID()))
      storageLinks <- storageLinksCache.get
      baseDirectory <- IO.fromEither(getLocalBaseDirectory(req.localObjectPath).leftMap(s => BadRequestException(s)))
      storageLink = storageLinks.find(x => x._1.path == baseDirectory)
      _ <- storageLink.fold[IO[Unit]](IO.raiseError(StorageLinkNotFoundException(s"No storage link found for ${req.localObjectPath}"))) { pair =>
        val sl = pair._2
        for {
          previousMeta <- metadataCache.get.map(_.get(req.localObjectPath))
          fullBlobName <- IO.fromEither(getFullBlobName(req.localObjectPath, sl.cloudStorageDirectory.blobPath).leftMap(s => BadRequestException(s)))
          latestMetadata <- retrieveGcsMetadata(sl.cloudStorageDirectory.bucketName, fullBlobName, traceId)
          gsPath = GsPath(sl.cloudStorageDirectory.bucketName, fullBlobName)
          _ <- (previousMeta, latestMetadata) match {
            case (Some(prev), Some(now)) =>
              if(prev.crc32c == now.crc32c)
                delocalize(req.localObjectPath, gsPath, traceId, prev.generation)
              else
                IO.raiseError(FileOutOfSyncException("file is updated in google storage by someone else"))
            case (_, None) => // if file is deleted in remote, we'll just delocalize the file
              delocalize(req.localObjectPath, gsPath, traceId, 0L)
            case (None, Some(now)) =>
              IO.raiseError(new Exception("Can't determine if the file user started off of is up-to-date"))
          }
        } yield ()
      }
    } yield ()

  private def delocalize(localObjectPath: java.nio.file.Path, gsPath: GsPath, traceId: TraceId, generation: Long): IO[Unit] =
    io.file.readAll[IO](localObjectPath, blockingEc, 4096).compile.to[Array].flatMap { body =>
      for {
         current <- timer.clock.realTime(TimeUnit.MILLISECONDS)
         metadata = Map("lastLockedBy" -> config.ownerEmail, "expiresAt" -> (current + config.lockExpiration.toMillis).toString)
         _ <- googleStorageService
          .storeObject(gsPath.bucketName, gsPath.blobName, body, gcpObjectType, Map.empty, Some(generation), Some(traceId))
          .compile
          .drain
      } yield ()
    }
}

object ObjectService {
  def apply(config: ObjectServiceConfig, googleStorageService: GoogleStorageService[IO], blockingEc: ExecutionContext, storageLinksCache: StorageLinksCache, metadataCache: MetadataCache)(
      implicit cs: ContextShift[IO],
      logger: Logger[IO],
      timer: Timer[IO]
  ): ObjectService = new ObjectService(config, googleStorageService, blockingEc, storageLinksCache, metadataCache)

  implicit val actionDecoder: Decoder[Action] = Decoder.decodeString.emap { str =>
    Action.stringToAction.get(str).toRight("invalid action")
  }

  implicit val entryDecoder: Decoder[Entry] = Decoder.instance { cursor =>
    for {
      bucketAndObject <- cursor.downField("sourceUri").as[SourceUri]
      localObjectPath <- cursor.downField("localDestinationPath").as[Path]
    } yield Entry(bucketAndObject, localObjectPath)
  }

  implicit val localizeDecoder: Decoder[Localize] = Decoder.forProduct1("entries") {
    Localize.apply
  }

  implicit val safeDelocalizeDecoder: Decoder[SafeDelocalize] = Decoder.forProduct1("localPath") {
    SafeDelocalize.apply
  }

  implicit val postObjectRequestDecoder: Decoder[PostObjectRequest] = Decoder.instance { cursor =>
    for {
      action <- cursor.downField("action").as[Action]
      req <- action match {
        case Action.Localize =>
          cursor.as[Localize]
        case Action.SafeDelocalize =>
          cursor.as[SafeDelocalize]
      }
    } yield req
  }

  implicit val getMetadataDecoder: Decoder[GetMetadataRequest] = Decoder.forProduct1("localPath")(GetMetadataRequest.apply)

  implicit val metadataResponseEditModeEncoder: Encoder[MetadataResponse.EditMode] = Encoder.forProduct5(
    "syncMode",
    "syncStatus",
    "lastLockedBy",
    "isExecutionMode",
    "storageLink"
  )(x => (x.syncMode, x.syncStatus, x.lastLockedBy, x.isExecutionMode, x.storageLinks))

  implicit val metadataResponseRemoteNotFoundEncoder: Encoder[MetadataResponse.RemoteNotFound] = Encoder.forProduct4(
    "syncMode",
    "syncStatus",
    "isExecutionMode",
    "storageLink"
  )(x => (x.syncMode, x.syncStatus, x.isExecutionMode, x.storageLinks))

  implicit val metadataResponseEncoder: Encoder[MetadataResponse] = Encoder.instance { x =>
    x match {
      case MetadataResponse.SafeMode => Map("syncMode" -> SyncMode.Safe.toString).asJson
      case x: MetadataResponse.EditMode => x.asJson
      case x: MetadataResponse.RemoteNotFound => x.asJson
    }
  }
}

final case class GetMetadataRequest(localObjectPath: Path)
sealed abstract class Action
object Action {
  final case object Localize extends Action {
    override def toString: String = "localize"
  }
  final case object SafeDelocalize extends Action {
    override def toString: String = "safeDelocalize"
  }

  val stringToAction: Map[String, Action] = sealerate.values[Action].map(a => a.toString -> a).toMap
}
sealed abstract class PostObjectRequest extends Product with Serializable {
  def action: Action
}
object PostObjectRequest {
  final case class Localize(entries: List[Entry]) extends PostObjectRequest {
    override def action: Action = Action.Localize
  }
  final case class SafeDelocalize(localObjectPath: Path) extends PostObjectRequest {
    override def action: Action = Action.SafeDelocalize
  }
}

sealed abstract class MetadataResponse extends Product with Serializable {
  def syncMode: SyncMode
}
object MetadataResponse {
  case object SafeMode extends MetadataResponse {
    def syncMode: SyncMode = SyncMode.Safe
  }

  final case class EditMode(syncStatus: SyncStatus, lastLockedBy: Option[WorkbenchEmail], isExecutionMode: Boolean, generation: Long, storageLinks: StorageLink)
      extends MetadataResponse {
    def syncMode: SyncMode = SyncMode.Edit
  }

  final case class RemoteNotFound(isExecutionMode: Boolean, storageLinks: StorageLink) extends MetadataResponse {
    def syncMode: SyncMode = SyncMode.Edit
    def syncStatus: SyncStatus = SyncStatus.RemoteNotFound
  }
}

final case class Entry(sourceUri: SourceUri, localObjectPath: Path)

final case class LocalizeRequest(entries: List[Entry])

final case class ObjectServiceConfig(workingDirectory: Path, ownerEmail: WorkbenchEmail, lockExpiration: FiniteDuration)