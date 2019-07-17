package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

import _root_.fs2.{Stream, io}
import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.circe.syntax._
import _root_.io.circe.{Decoder, Encoder}
import ca.mrvisser.sealerate
import cats.data.Kleisli
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
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
)(implicit cs: ContextShift[IO], timer: Timer[IO], logger: Logger[IO])
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
        resp <- NoContent()
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
          case DataUri(data) => Stream.emits(data).through(io.file.writeAll[IO](localAbsolutePath, blockingEc, writeFileOptions))
          case gsPath: GsPath =>
            for {
              meta <- googleStorageAlg.gcsToLocalFile(localAbsolutePath, gsPath, traceId).last
              _ <- meta.fold[Stream[IO, Unit]](Stream.raiseError[IO](NotFoundException(s"${gsPath} not found")))(m => Stream.eval(updateCache(entry.localObjectPath, m)))
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
  def acquireLock(req: AcquireLockRequest, traceId: TraceId): IO[Unit] =
    for {
      context <- storageLinksAlg.findStorageLink(req.localObjectPath)
      current <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      gsPath = getGsPath(req.localObjectPath, context)
      hashedLockedByCurrentUser <- IO.fromEither(hashString(lockedByString(gsPath.bucketName, config.ownerEmail)))
      metadata = lockMetadata(current + config.lockExpiration.toMillis, hashedLockedByCurrentUser)
      meta <- googleStorageAlg.retrieveAdaptedGcsMetadata(req.localObjectPath, gsPath, traceId)
      res <- meta match {
        case Some(m) =>
          for {
            _ <- updateLockCache(req.localObjectPath, m.lock)
            res <- m.lock match {
              case Some(lock) =>
                if (lock.lastLockedBy == hashedLockedByCurrentUser)
                  updateGcsMetadataAndCache(req.localObjectPath, gsPath, traceId, metadata).void
                else
                  IO.raiseError(LockedByOther(s"lock is already acquired by someone else"))
              case None =>
                updateGcsMetadataAndCache(req.localObjectPath, gsPath, traceId, metadata).void
            }
          } yield res
        case None =>
          IO.raiseError(NotFoundException(s"${gsPath} not found in Google Storage"))
      }
    } yield res

  private def updateGcsMetadataAndCache(localPath: RelativePath, gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[Unit] = {
    googleStorageAlg.updateMetadata(gsPath, traceId, metadata).flatMap(
      updateMetadataResponse =>
        updateMetadataResponse match {
          case UpdateMetadataResponse.DirectMetadataUpdate => IO.unit
          case UpdateMetadataResponse.ReUploadObject(generation, crc32c) => updateLocalFileStateCache(localPath, LocalFileStateInGCS(crc32c, generation))
        }
    )
  }

  /**
    * In case you're wondering what Kleisli is, Kleisli is a data type that wraps a function (A => F[B]), and commonly used for abstracting away some type of dependency.
    * For instance, in this method, we're abstracting away the need to pass around `traceId` explicitly.
    * For more information about Kleisli, check out https://typelevel.org/cats/datatypes/kleisli.html.
    * Even though you don't see many Kleisli used explicitly in welder, it's actually used extensively, because it's a fundamental data type
    * used by http4s, the web library welder depends on.
    */
  def checkMetadata(req: GetMetadataRequest): Kleisli[IO, TraceId, MetadataResponse] =
    for {
      traceId <- Kleisli.ask[IO, TraceId]
      context <- Kleisli.liftF(storageLinksAlg.findStorageLink(req.localObjectPath))
      res <- if (context.isSafeMode)
        Kleisli.pure[IO, TraceId, MetadataResponse](MetadataResponse.SafeMode(context.storageLink))
      else {
        val fullBlobName = getFullBlobName(context.basePath, req.localObjectPath.asPath, context.storageLink.cloudStorageDirectory.blobPath)
        for {
          metadata <- Kleisli.liftF[IO, TraceId, Option[AdaptedGcsMetadata]](
            googleStorageAlg
              .retrieveAdaptedGcsMetadata(req.localObjectPath, GsPath(context.storageLink.cloudStorageDirectory.bucketName, fullBlobName), traceId)
          )
          result <- metadata match {
            case None =>
              Kleisli.pure[IO, TraceId, MetadataResponse](MetadataResponse.RemoteNotFound(context.storageLink))
            case Some(AdaptedGcsMetadata(lock, crc32c, generation)) =>
              val localAbsolutePath = config.workingDirectory.resolve(req.localObjectPath.asPath)
              val res = for {
                calculatedCrc32c <- Crc32c.calculateCrc32ForFile(localAbsolutePath, blockingEc)
                syncStatus <- if (calculatedCrc32c == crc32c) IO.pure(SyncStatus.Live)
                else {
                  for {
                    metaOpt <- metadataCache.get.map(_.get(req.localObjectPath))
                    loggingContext = MetaLoggingContext(traceId, "checkMetadata", context, metaOpt.flatMap(_.localFileStateInGCS.map(_.generation)), generation)
                    status <- metaOpt match {
                      case Some(meta) =>
                        meta.localFileStateInGCS match { //TODO: shall we check the file has been localized before calculating crc32c
                          case Some(previousFileState) =>
                            if (previousFileState.generation == generation)
                              IO.pure(SyncStatus.LocalChanged) <* logger
                                .ctxWarn[MetaLoggingContext, String](
                                  s"local file has changed, but it hasn't been delocalized yet"
                                )
                                .run(loggingContext)
                            else IO.pure(SyncStatus.RemoteChanged) <* logger.ctxWarn[MetaLoggingContext, String]("remote has changed").run(loggingContext)
                          case None =>
                            IO.pure(SyncStatus.Desynchronized) <* logger
                              .ctxError[MetaLoggingContext, String]("We don't find local generation for a localized file. Was this file localized manually?")
                              .run(loggingContext)
                        }
                      case None =>
                        IO.pure(SyncStatus.Desynchronized) <* logger
                          .ctxError[MetaLoggingContext, String]("We don't find local cache for a localized file. Did you update metadata cache file directly?")
                          .run(loggingContext) //TODO: this shouldn't be possible because we should have the file in cache if it has been localized
                    }
                  } yield status
                }
                _ <- updateLockCache(req.localObjectPath, lock)
              } yield MetadataResponse.EditMode(syncStatus, lock.map(_.lastLockedBy), generation, context.storageLink)
              Kleisli.liftF[IO, TraceId, MetadataResponse](res)
          }
        } yield result
      }
    } yield res

  def safeDelocalize(req: SafeDelocalize): Kleisli[IO, TraceId, Unit] =
    for {
      traceId <- Kleisli.ask[IO, TraceId]
      context <- Kleisli.liftF(storageLinksAlg.findStorageLink(req.localObjectPath))
      loggingContext = PostContext(traceId, "safeDelocalize", context)
      _ <- if (context.isSafeMode)
        Kleisli.liftF[IO, TraceId, Unit](IO.raiseError(SafeDelocalizeSafeModeFileError(s"${req.localObjectPath} can't be delocalized since it's in safe mode")))
      else {
        val res: IO[Unit] = for {
          previousMeta <- metadataCache.get.map(_.get(req.localObjectPath))
          gsPath = getGsPath(req.localObjectPath, context)
          now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
          // If we have lock info in local cache, we check cache to see if we hold a valid lock.
          // local cache should be updated pretty frequently since acquireLock is called much more often than lock expires.
          // Hence, we should be able to rely on cache for checking lock
          lockToPush <- previousMeta match {
            case Some(meta) =>
              checkLock(meta.lock, now, gsPath.bucketName)
            case None =>
              // If this is a new file, acquire lock for current user; If the file actually exists in GCE, delocalize will fail with generation mismatch.
              for {
                hashedLockedByCurrentUser <- IO.fromEither(hashString(lockedByString(gsPath.bucketName, config.ownerEmail)))
                current <- timer.clock.realTime(TimeUnit.MILLISECONDS)
              } yield lockMetadata(current + config.lockExpiration.toMillis, hashedLockedByCurrentUser)
          }
          generation = previousMeta.flatMap(_.localFileStateInGCS.map(_.generation)).getOrElse(0L)
          delocalizeResp <- googleStorageAlg.delocalize(req.localObjectPath, gsPath, generation, lockToPush, traceId)
          _ <- updateLocalFileStateCache(req.localObjectPath, LocalFileStateInGCS(delocalizeResp.crc32c, delocalizeResp.generation))
        } yield ()

        Kleisli.liftF(res)
      }
    } yield ()

  def delete(req: Delete): Kleisli[IO, TraceId, Unit] =
    for {
      traceId <- Kleisli.ask[IO, TraceId]
      context <- Kleisli.liftF(storageLinksAlg.findStorageLink(req.localObjectPath))
      _ <- if (context.isSafeMode)
        Kleisli.liftF[IO, TraceId, Unit](IO.raiseError(DeleteSafeModeFileError(s"${req.localObjectPath} can't be deleted since it's in safe mode")))
      else {
        val gsPath = getGsPath(req.localObjectPath, context)
        val res = for {
          previousMeta <- metadataCache.get.map(_.get(req.localObjectPath))
          now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
          generation <- previousMeta match {
            case Some(meta) =>
              checkLock(meta.lock, now, gsPath.bucketName)
                .as(meta.localFileStateInGCS.map(_.generation).getOrElse(0L)) //check if user owns the lock before deleting
            case None =>
              IO.raiseError(InvalidLock(s"Local GCS metadata for ${req.localObjectPath} not found"))
          }
          _ <- googleStorageAlg.removeObject(gsPath, traceId, Some(generation)).compile.drain.void
          _ <- metadataCache.modify(mp => (mp - req.localObjectPath, ())) // remove the object from cache
        } yield ()
        Kleisli.liftF(res)
      }
    } yield ()

  /**
    * If lock exists and it hasn't expired, we keep the lock; if lock doesn't exist, we return empty Map as metadata
    * @return metadata to push if lock is valid
    */
  private def checkLock(lock: Option[Lock], now: Long, bucketName: GcsBucketName): IO[Map[String, String]] =
    for {
      hashedLockedByCurrentUser <- IO.fromEither(hashString(lockedByString(bucketName, config.ownerEmail)))
      res <- lock match {
        case Some(lock) =>
          if (now <= lock.lockExpiresAt.toEpochMilli && lock.lastLockedBy == hashedLockedByCurrentUser) //if current user holds the lock
            IO.pure(lockMetadata(lock.lockExpiresAt.toEpochMilli, hashedLockedByCurrentUser))
          else IO.raiseError(InvalidLock("Fail to delocalize due to lock expiration or lock held by someone else"))
        case None => IO.pure(Map.empty[String, String])
      }
    } yield res

  private def updateLockCache(localPath: RelativePath, lock: Option[Lock]): IO[Unit] =
    metadataCache.modify { mp =>
      val previousMeta = mp.get(localPath)
      val newCache = mp + (localPath -> AdaptedGcsMetadataCache(localPath, lock, previousMeta.flatMap(_.localFileStateInGCS)))
      (newCache, ())
    }

  private def updateLocalFileStateCache(localPath: RelativePath, localFileStateInGCS: LocalFileStateInGCS): IO[Unit] =
    metadataCache.modify { mp =>
      val previousMeta = mp.get(localPath)
      val newCache = mp + (localPath -> AdaptedGcsMetadataCache(localPath, previousMeta.flatMap(_.lock), Some(localFileStateInGCS)))
      (newCache, ())
    }

  private def updateCache(localPath: RelativePath, adaptedGcsMetadata: AdaptedGcsMetadata): IO[Unit] =
    metadataCache.modify { mp =>
      val newCache = mp + (localPath -> AdaptedGcsMetadataCache(
        localPath,
        adaptedGcsMetadata.lock,
        Some(LocalFileStateInGCS(adaptedGcsMetadata.crc32c, adaptedGcsMetadata.generation))
      ))
      (newCache, ())
    }

  private def lockMetadata(lockExpiresAt: Long, hashedLockedBy: HashedLockedBy): Map[String, String] = Map(
    GoogleStorageAlg.LAST_LOCKED_BY -> hashedLockedBy.asString,
    GoogleStorageAlg.LOCK_EXPIRES_AT -> lockExpiresAt.toString
  )
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
      timer: Timer[IO],
      logger: Logger[IO]
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

  implicit val postContextEncoder: Encoder[PostContext] = Encoder.forProduct4(
    "traceId",
    "action",
    "isSafeMode",
    "basePath"
  )(x => (x.traceId, x.action, x.commonContext.isSafeMode, x.commonContext.basePath))

  implicit val metaLoggingContextEncoder: Encoder[MetaLoggingContext] = Encoder.forProduct6(
    "traceId",
    "action",
    "isSafeMode",
    "basePath",
    "oldGeneration",
    "newGeneration"
  )(x => (x.traceId, x.action, x.commonContext.isSafeMode, x.commonContext.basePath, x.oldGeneration, x.newGeneration))
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

  final case class EditMode(syncStatus: SyncStatus, lastLockedBy: Option[HashedLockedBy], generation: Long, storageLink: StorageLink) extends MetadataResponse {
    def syncMode: SyncMode = SyncMode.Edit
  }

  final case class RemoteNotFound(storageLink: StorageLink) extends MetadataResponse {
    def syncMode: SyncMode = SyncMode.Edit
    def syncStatus: SyncStatus = SyncStatus.RemoteNotFound
  }
}

final case class Entry(sourceUri: SourceUri, localObjectPath: RelativePath)

final case class LocalizeRequest(entries: List[Entry])

final case class ObjectServiceConfig(
    workingDirectory: Path, //root directory where all local files will be mounted
    ownerEmail: WorkbenchEmail,
    lockExpiration: FiniteDuration
)

final case class GsPathAndMetadata(gsPath: GsPath, metadata: AdaptedGcsMetadata)

final case class AcquireLockRequest(localObjectPath: RelativePath)

final case class PostContext(traceId: TraceId, action: String, commonContext: CommonContext)
final case class MetaLoggingContext(traceId: TraceId, action: String, commonContext: CommonContext, oldGeneration: Option[Long], newGeneration: Long)
