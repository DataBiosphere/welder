package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.file.Path
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

import _root_.fs2.{Stream, io}
import _root_.io.circe.syntax._
import _root_.io.circe.{Decoder, Encoder}
import ca.mrvisser.sealerate
import cats.data.Kleisli
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.{DataUri, GsPath}
import org.broadinstitute.dsp.workbench.welder.server.ObjectService._
import org.broadinstitute.dsp.workbench.welder.server.PostObjectRequest._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class ObjectService(
                     config: ObjectServiceConfig,
                     googleStorageAlg: GoogleStorageAlg,
                     blockingEc: ExecutionContext,
                     storageLinksAlg: StorageLinksAlg,
                     metadataCache: MetadataCache
)(implicit cs: ContextShift[IO], timer: Timer[IO])
    extends Http4sDsl[IO] {
  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ POST -> Root / "metadata" =>
      for {
        traceId <- IO(TraceId(randomUUID()))
        metadataReq <- req.as[GetMetadataRequest]
        res <- checkMetadata(metadataReq).run(traceId)
        resp <- Ok(res)
      } yield resp
    case req @ POST -> Root / "lock" =>
      for {
        traceId <- IO(TraceId(randomUUID()))
        request <- req.as[AcquireLockRequest]
        res <- acquireLock(request, traceId)
        resp <- Ok(res)
      } yield resp
    case req @ POST -> Root =>
      for {
        traceId <- IO(TraceId(randomUUID()))
        localizeReq <- req.as[PostObjectRequest]
        res <- localizeReq match {
          case x: Localize => localize(x).run(traceId) >> NoContent()
          case x: SafeDelocalize => safeDelocalize(x).run(traceId) >> NoContent()
          case x: Delete => delete(x).run(traceId) >> NoContent()
        }
      } yield res
  }

  def localize(req: Localize): Kleisli[IO, TraceId, Unit] = Kleisli { traceId =>
    val res = Stream
      .emits(req.entries)
      .map { entry =>
        val localAbsolutePath = config.workingDirectory.resolve(entry.localObjectPath.asPath)

        val localizeFile = entry.sourceUri match {
          case DataUri(data) => Stream.emits(data).through(io.file.writeAll[IO](localAbsolutePath, blockingEc))
          case gsPath: GsPath =>
            for {
              meta <- googleStorageAlg.gcsToLocalFile(localAbsolutePath, gsPath, traceId)
              metaInCache = AdaptedGcsMetadataCache(entry.localObjectPath, meta.lastLockedBy, meta.crc32c, meta.generation)
              _ <- Stream.eval(metadataCache.modify(mp => (mp + (entry.localObjectPath.asPath -> metaInCache), ())))
            } yield ()
        }

        Stream.eval(mkdirIfNotExist(localAbsolutePath.getParent)) >> localizeFile
      }
      .parJoin(10)

    res.compile.drain
  }

  //Workspace buckets prior to Jun 2019 use legacy ACLs. Unfortunately, legacy ACLs do not contain the storage.objects.update permission,
  //which is required to update object metadata in GCS. The acquireLock code below contains the following workaround:
  // 1. Attempt to update the GCS object metadata directly.
  //    a. If this succeeds, great!
  //    b. If this fails, proceed to step 2
  // 2. Overwrite the object completely with the metadata. This results in extra network traffic, but by doing so the user will gain ownership of
  //    the object in GCS, which will grant them the storage.objects.update permission, which will allow Step 1 to succeed for all subsequent calls.
  // Note: Step 2 should only occur if we're operating in a workspace bucket that was created with legacy ACLs. New buckets use modern ACLs and have
  //       bucket policy only enabled, which will allow step 1 to succeed.
  //
  def acquireLock(req: AcquireLockRequest, traceId: TraceId): IO[AcquireLockResponse] = {
    for {
      context <- storageLinksAlg.findStorageLink(req.localObjectPath)
      current <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      metadata = Map(GoogleStorageAlg.LAST_LOCKED_BY -> config.ownerEmail.value,
        GoogleStorageAlg.LOCK_EXPIRES_AT -> (current + config.lockExpiration.toMillis).toString)
      gsPath = getGsPath(req.localObjectPath, context)
      meta <- googleStorageAlg.retrieveAdaptedGcsMetadata(req.localObjectPath, gsPath, traceId)
      res <- meta match {
        case Some(m) =>
          if(m.lastLockedBy.isDefined && m.lastLockedBy.get == config.ownerEmail)
            googleStorageAlg.updateMetadata(gsPath, traceId, metadata).as(AcquireLockResponse.Success)
          else
            IO.pure(AcquireLockResponse.LockedByOther)
        case None =>
          googleStorageAlg.updateMetadata(gsPath, traceId, metadata).as(AcquireLockResponse.Success)
      }
    } yield res
  }

  /**
    * In case you're wondering what Kleisli is, Kleisli is a data type that wraps a function (A => F[B]), and commonly used for abstracting away some type of dependency.
    * For instance, in this method, we're abstracting away the need to pass around `traceId` explicitly.
    * For more information about Kleisli, check out https://typelevel.org/cats/datatypes/kleisli.html.
    * Even though you don't see many Kleisli used explicitly in welder, it's actually used extensively, because it's a fundamental data type
    * used by http4s, the web library welder depends on.
   */
  def checkMetadata(req: GetMetadataRequest): Kleisli[IO, TraceId, MetadataResponse] = {
    for {
      traceId <- Kleisli.ask[IO, TraceId]
      context <- Kleisli.liftF(storageLinksAlg.findStorageLink(req.localObjectPath))
      res <- if(context.isSafeMode)
        Kleisli.pure[IO, TraceId, MetadataResponse](MetadataResponse.SafeMode(context.storageLink))
      else {
        val fullBlobName = getFullBlobName(context.basePath, req.localObjectPath.asPath, context.storageLink.cloudStorageDirectory.blobPath)
        for {
          metadata <- Kleisli.liftF[IO, TraceId, Option[AdaptedGcsMetadata]](googleStorageAlg.retrieveAdaptedGcsMetadata(req.localObjectPath, GsPath(context.storageLink.cloudStorageDirectory.bucketName, fullBlobName), traceId))
          result <- metadata match {
            case None =>
              Kleisli.pure[IO, TraceId, MetadataResponse](MetadataResponse.RemoteNotFound(context.storageLink))
            case Some(AdaptedGcsMetadata(lastLockedBy, crc32c, generation)) =>
              val localAbsolutePath = config.workingDirectory.resolve(req.localObjectPath.asPath)
              val res = for {
                calculatedCrc32c <- Crc32c.calculateCrc32ForFile(localAbsolutePath, blockingEc)
                syncStatus <- if (calculatedCrc32c == crc32c) IO.pure(SyncStatus.Live) else {
                  for {
                    cachedGeneration <- metadataCache.get.map(_.get(req.localObjectPath.asPath))
                    status <- cachedGeneration match {
                      case Some(cachedGen) => if(cachedGen.generation == generation) IO.pure(SyncStatus.LocalChanged) else IO.pure(SyncStatus.RemoteChanged)
                      case None => IO.pure(SyncStatus.Desynchronized)
                    }
                  } yield status
                }
              } yield MetadataResponse.EditMode(syncStatus, lastLockedBy, generation, context.storageLink)
              Kleisli.liftF[IO, TraceId, MetadataResponse](res)
          }
        } yield result
      }
    } yield res
  }

  def safeDelocalize(req: SafeDelocalize): Kleisli[IO, TraceId, Unit] = {
    for {
      traceId <- Kleisli.ask[IO, TraceId]
      context <- Kleisli.liftF(storageLinksAlg.findStorageLink(req.localObjectPath))
      _ <- if(context.isSafeMode)
        Kleisli.liftF[IO, TraceId, Unit](IO.raiseError(SafeDelocalizeSafeModeFileError(s"${req.localObjectPath} can't be delocalized since it's in safe mode")))
      else for {
        previousMeta <- Kleisli.liftF(metadataCache.get.map(_.get(req.localObjectPath.asPath)))
        gsPath = getGsPath(req.localObjectPath, context)
        delocalizeResp <- previousMeta match {
          case Some(m) => Kleisli.liftF[IO, TraceId, DelocalizeResponse](googleStorageAlg.delocalize(req.localObjectPath, gsPath, m.generation, traceId))
          case None => Kleisli.liftF[IO, TraceId, DelocalizeResponse](googleStorageAlg.delocalize(req.localObjectPath, gsPath, 0L, traceId))
        }
        adaptedGcsMetadata = AdaptedGcsMetadataCache(req.localObjectPath, Some(config.ownerEmail), delocalizeResp.crc32c, delocalizeResp.generation)
        _ <- Kleisli.liftF(metadataCache.modify(mp => (mp + (req.localObjectPath.asPath -> adaptedGcsMetadata), ())))
      } yield ()
    } yield ()
  }

  def delete(req: Delete): Kleisli[IO, TraceId, Unit] = {
    for {
      traceId <- Kleisli.ask[IO, TraceId]
      context <- Kleisli.liftF(storageLinksAlg.findStorageLink(req.localObjectPath))
      _ <- if(context.isSafeMode)
          Kleisli.liftF[IO, TraceId, Unit](IO.raiseError(DeleteSafeModeFileError(s"${req.localObjectPath} can't be deleted since it's in safe mode")))
        else {
          val gsPath = getGsPath(req.localObjectPath, context)
          for {
           previousMeta <- Kleisli.liftF(metadataCache.get.map(_.get(req.localObjectPath.asPath)))
           _ <- previousMeta match {
             case Some(m) => Kleisli.liftF[IO, TraceId, Unit](googleStorageAlg.removeObject(gsPath, traceId, Some(m.generation)).compile.drain.void)
             case None => Kleisli.liftF[IO, TraceId, Unit](IO.raiseError(UnknownFileState(s"Local GCS metadata for ${req.localObjectPath} not found")))
           }
          } yield ()
        }
    } yield ()
  }

  private def mkdirIfNotExist(path: java.nio.file.Path): IO[Unit] = {
    val directory = new File(path.toString)
    if (!directory.exists) {
      IO(directory.mkdirs).void
    } else IO.unit
  }
}

object ObjectService {
  def apply(
             config: ObjectServiceConfig,
             googleStorageAlg: GoogleStorageAlg,
             blockingEc: ExecutionContext,
             storageLinksAlg: StorageLinksAlg,
             metadataCache: MetadataCache
           )(
      implicit cs: ContextShift[IO],
      timer: Timer[IO]
  ): ObjectService = new ObjectService(config, googleStorageAlg, blockingEc, storageLinksAlg, metadataCache)

  implicit val actionDecoder: Decoder[Action] = Decoder.decodeString.emap { str =>
    Action.stringToAction.get(str).toRight("invalid action")
  }

  implicit val entryDecoder: Decoder[Entry] = Decoder.instance { cursor =>
    for {
      bucketAndObject <- cursor.downField("sourceUri").as[SourceUri]
      localObjectPath <- cursor.downField("localDestinationPath").as[RelativePath]
    } yield Entry(bucketAndObject, localObjectPath)
  }

  implicit val localizeDecoder: Decoder[Localize] = Decoder.forProduct1("entries") {
    Localize.apply
  }

  implicit val safeDelocalizeDecoder: Decoder[SafeDelocalize] = Decoder.forProduct1("localPath") {
    SafeDelocalize.apply
  }

  implicit val deleteDelocalizeDecoder: Decoder[Delete] = Decoder.forProduct1("localPath") {
    Delete.apply
  }

  implicit val postObjectRequestDecoder: Decoder[PostObjectRequest] = Decoder.instance { cursor =>
    for {
      action <- cursor.downField("action").as[Action]
      req <- action match {
        case Action.Localize =>
          cursor.as[Localize]
        case Action.SafeDelocalize =>
          cursor.as[SafeDelocalize]
        case Action.Delete =>
          cursor.as[Delete]
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
  implicit val acquireLockRequestDecoder: Decoder[AcquireLockRequest] = Decoder.forProduct1("localPath")(AcquireLockRequest.apply)

  implicit val acquireLockResponseEncoder: Encoder[AcquireLockResponse] = Encoder.forProduct1("result")(x => x.result)
}

final case class GetMetadataRequest(localObjectPath: RelativePath)
sealed abstract class Action
object Action {
  final case object Localize extends Action {
    override def toString: String = "localize"
  }
  final case object SafeDelocalize extends Action {
    override def toString: String = "safeDelocalize"
  }
  final case object Delete extends Action {
    override def toString: String = "delete"
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
  final case class SafeDelocalize(localObjectPath: RelativePath) extends PostObjectRequest {
    override def action: Action = Action.SafeDelocalize
  }
  final case class Delete(localObjectPath: RelativePath) extends PostObjectRequest {
    override def action: Action = Action.Delete
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

final case class Entry(sourceUri: SourceUri, localObjectPath: RelativePath)

final case class LocalizeRequest(entries: List[Entry])

final case class ObjectServiceConfig(workingDirectory: Path, //root directory where all local files will be mounted
                                     ownerEmail: WorkbenchEmail,
                                     lockExpiration: FiniteDuration)

final case class GsPathAndMetadata(gsPath: GsPath, metadata: AdaptedGcsMetadata)

final case class AcquireLockRequest(localObjectPath: RelativePath)

sealed abstract class AcquireLockResponse {
  def result: String
}
object AcquireLockResponse {
  final case object LockedByOther extends AcquireLockResponse {
    def result: String = "LOCKED_BY_OTHER"
  }
  final case object Success extends AcquireLockResponse {
    def result: String = "SUCCESS"
  }
}