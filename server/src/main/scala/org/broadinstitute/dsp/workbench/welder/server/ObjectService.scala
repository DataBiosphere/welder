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
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
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
              meta <- Stream.eval(retrieveGcsMetadata(entry.localObjectPath, bucketName, blobName, traceId))
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

  private def retrieveGcsMetadata(localPath: java.nio.file.Path, bucketName: GcsBucketName, blobName: GcsBlobName, traceId: TraceId): IO[Option[GcsMetadata]] = for {
    meta <- googleStorageService.getObjectMetadata(bucketName, blobName, Some(traceId)).compile.last
    res <- meta match {
      case Some(google2.GetMetadataResponse.Metadata(crc32c, userDefinedMetadata, generation)) =>
        for {
          lastLockedBy <- IO.pure(userDefinedMetadata.get("lastLockedBy").map(WorkbenchEmail))
          expiresAt <- userDefinedMetadata.get("lockExpiresAt").flatTraverse {
            expiresAtString =>
              for {
                ea <- Either.catchNonFatal(expiresAtString.toLong).fold[IO[Option[Long]]](_ => logger.warn(s"Failed to convert $expiresAtString to epoch millis") *> IO.pure(None), l => IO.pure(Some(l)))
                instant = ea.map(Instant.ofEpochMilli)
              } yield instant
          }
        } yield Some(GcsMetadata(localPath, lastLockedBy, expiresAt, crc32c, generation))
      case _ => IO.pure(None)
    }
  } yield res

  def checkMetadata(req: GetMetadataRequest): IO[MetadataResponse] =
    for {
      traceId <- IO(TraceId(randomUUID()))
      storageLink <- findStorageLink(req.localObjectPath)
      res <- storageLink.fold[IO[MetadataResponse]](IO.raiseError(StorageLinkNotFoundException(s"No storage link found for ${req.localObjectPath}"))) { pair =>
        val isSafeMode = pair._1
        val sl = pair._2
        if(isSafeMode)
          IO.pure(MetadataResponse.SafeMode(sl))
        else {
          for {
            fullBlobName <- IO.fromEither(getFullBlobName(req.localObjectPath, sl.cloudStorageDirectory.blobPath).leftMap(s => BadRequestException(s)))
            metadata <- retrieveGcsMetadata(req.localObjectPath, sl.cloudStorageDirectory.bucketName, fullBlobName, traceId)
            result <- metadata match {
              case None =>
                IO(MetadataResponse.RemoteNotFound(sl))
              case Some(GcsMetadata(_, lastLockedBy, expiresAt, crc32c, generation)) =>
                val localAbsolutePath = config.workingDirectory.resolve(req.localObjectPath)
                for {
                  fileBody <- io.file.readAll[IO](localAbsolutePath, blockingEc, 4096).compile.to[Array]
                  calculatedCrc32c = Crc32c.calculateCrc32c(fileBody)
                  syncStatus <- if (calculatedCrc32c == crc32c) IO.pure(SyncStatus.Live) else {
                    for {
                      cachedGeneration <- metadataCache.get.map(_.get(req.localObjectPath))
                      status <- cachedGeneration match {
                        case Some(cachedGen) => if(cachedGen.generation == generation) IO.pure(SyncStatus.LocalChanged) else IO.pure(SyncStatus.RemoteChanged)
                        case None => IO.pure(SyncStatus.OutOfSync)
                      }
                    } yield status
                  }
                  currentTime <- timer.clock.realTime(TimeUnit.MILLISECONDS)
                  lastLock <- expiresAt.flatTraverse[IO, WorkbenchEmail] { ea =>
                    if (currentTime > ea.toEpochMilli)
                      IO.pure(none[WorkbenchEmail])
                    else
                      IO.pure(lastLockedBy)
                  }
                } yield MetadataResponse.EditMode(syncStatus, lastLock, generation, sl)
            }
          } yield result
        }
      }
    } yield res

  def safeDelocalize(req: SafeDelocalize): IO[Unit] =
    for {
      traceId <- IO(TraceId(randomUUID()))
      storageLink <- findStorageLink(req.localObjectPath)
      _ <- storageLink.fold[IO[Unit]](IO.raiseError(StorageLinkNotFoundException(s"No storage link found for ${req.localObjectPath}"))) { pair =>
        val isSafeMode = pair._1
        val sl = pair._2
        if(isSafeMode)
            IO.unit
        else {
          for {
            previousMeta <- metadataCache.get.map(_.get(req.localObjectPath))
            fullBlobName <- IO.fromEither(getFullBlobName(req.localObjectPath, sl.cloudStorageDirectory.blobPath).leftMap(s => BadRequestException(s)))
            gsPath = GsPath(sl.cloudStorageDirectory.bucketName, fullBlobName)
            localAbsolutePath = config.workingDirectory.resolve(req.localObjectPath)
            _ <- previousMeta match {
              case Some(m) => delocalize(localAbsolutePath, gsPath, traceId, m.generation)
              case None => delocalize(localAbsolutePath, gsPath, traceId, 0L)
            }
          } yield ()
        }
      }
    } yield ()

  private def delocalize(localAbsolutePath: java.nio.file.Path, gsPath: GsPath, traceId: TraceId, generation: Long): IO[Unit] =
    io.file.readAll[IO](localAbsolutePath, blockingEc, 4096).compile.to[Array].flatMap { body =>println("body: "+body)
      googleStorageService
          .storeObject(gsPath.bucketName, gsPath.blobName, body, gcpObjectType, Map.empty, Some(generation), Some(traceId))
          .compile
          .drain
    }

  private def findStorageLink(localPath: java.nio.file.Path): IO[Option[(Boolean, StorageLink)]] = for {
    storageLinks <- storageLinksCache.get
    baseDirectory <- IO.fromEither(getLocalBaseDirectory(localPath).leftMap(s => BadRequestException(s)))
  } yield {
    storageLinks.find(x => x._1.path == baseDirectory).map {
      pair =>
        pair._1 match {
          case LocalBaseDirectory(_) => (false, pair._2)
          case LocalSafeBaseDirectory(_) => (true, pair._2)
        }
    }
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

  implicit val metadataResponseEditModeEncoder: Encoder[MetadataResponse.EditMode] = Encoder.forProduct4(
    "syncMode",
    "syncStatus",
    "lastLockedBy",
    "storageLink"
  )(x => (x.syncMode, x.syncStatus, x.lastLockedBy, x.storageLink))

  implicit val metadataResponseRemoteNotFoundEncoder: Encoder[MetadataResponse.RemoteNotFound] = Encoder.forProduct3(
    "syncMode",
    "syncStatus",
    "storageLink"
  )(x => (x.syncMode, x.syncStatus, x.storageLink))

  implicit val metadataResponseSafeModeEncoder: Encoder[MetadataResponse.SafeMode] = Encoder.forProduct2(
    "syncMode",
    "storageLink"
  )(x => (x.syncMode, x.storageLink))

  implicit val metadataResponseEncoder: Encoder[MetadataResponse] = Encoder.instance { x =>
    x match {
      case x: MetadataResponse.SafeMode => x.asJson
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
  case class SafeMode(storageLink: StorageLink) extends MetadataResponse {
    def syncMode: SyncMode = SyncMode.Safe
  }

  final case class EditMode(syncStatus: SyncStatus, lastLockedBy: Option[WorkbenchEmail], generation: Long, storageLink: StorageLink)
      extends MetadataResponse {
    def syncMode: SyncMode = SyncMode.Edit
  }

  final case class RemoteNotFound(storageLink: StorageLink) extends MetadataResponse {
    def syncMode: SyncMode = SyncMode.Edit
    def syncStatus: SyncStatus = SyncStatus.RemoteNotFound
  }
}

final case class Entry(sourceUri: SourceUri, localObjectPath: Path)

final case class LocalizeRequest(entries: List[Entry])

final case class ObjectServiceConfig(workingDirectory: Path, ownerEmail: WorkbenchEmail, lockExpiration: FiniteDuration)