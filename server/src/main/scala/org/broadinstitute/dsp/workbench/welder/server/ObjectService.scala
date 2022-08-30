package org.broadinstitute.dsp.workbench.welder
package server

import _root_.fs2.Stream
import _root_.io.circe.syntax._
import _root_.io.circe.{Decoder, Encoder}
import _root_.org.typelevel.log4cats.StructuredLogger
import ca.mrvisser.sealerate
import cats.data.Kleisli
import cats.effect.std.Semaphore
import cats.effect.{IO, Ref, Resource}
import cats.implicits._
import cats.mtl.Ask
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.SourceUri.{DataUri, GsPath}
import org.broadinstitute.dsp.workbench.welder.server.ObjectService._
import org.broadinstitute.dsp.workbench.welder.server.PostObjectRequest._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._

import java.nio.file.Path
import java.time.Instant
import scala.concurrent.duration._

class ObjectService(
    permitsRef: Permits,
    config: ObjectServiceConfig,
    storageAlgRef: Ref[IO, CloudStorageAlg],
    storageLinksAlg: StorageLinksAlg,
    metadataCacheAlg: MetadataCacheAlg
)(implicit logger: StructuredLogger[IO])
    extends WelderService {
  val service: HttpRoutes[IO] = withTraceId {
    case req @ POST -> Root / "metadata" =>
      traceId =>
        implicit val traceIdImplicit = Ask.const[IO, TraceId](traceId)

        for {
          metadataReq <- req.as[GetMetadataRequest]
          res <- checkMetadata(metadataReq)
          resp <- Ok(res)
        } yield resp
    case req @ POST -> Root / "lock" =>
      traceId =>
        implicit val traceIdImplicit = Ask.const[IO, TraceId](traceId)
        for {
          request <- req.as[AcquireLockRequest]
          _ <- acquireLock(request)
          resp <- NoContent()
        } yield resp
    case req @ POST -> Root =>
      traceId =>
        implicit val traceIdImplicit = Ask.const[IO, TraceId](traceId)
        for {
          localizeReq <- req.as[PostObjectRequest]
          res <- localizeReq match {
            case x: Localize => localize(x).run(traceId) >> NoContent()
            case x: SafeDelocalize => safeDelocalize(x) >> NoContent()
            case x: Delete => delete(x) >> NoContent()
          }
        } yield res
  }

  def localize(req: Localize)(implicit ev: Ask[IO, TraceId]): Kleisli[IO, TraceId, Unit] = Kleisli { traceId =>
    val res = Stream
      .emits(req.entries)
      .map { entry =>
        val localAbsolutePath = config.workingDirectory.resolve(entry.localObjectPath.asPath)
        val fs2Path = fs2.io.file.Path.fromNioPath(localAbsolutePath)

        val localizeFile = entry.sourceUri match {
          case DataUri(data) => Stream.emits(data).through(Files[IO].writeAll(fs2Path, writeFileOptions))
          case _ =>
            for {
              storageAlg <- Stream.eval(storageAlgRef.get)
              meta <- storageAlg.gcsToLocalFile(localAbsolutePath, entry.sourceUri).last
              _ <- meta.flatten.fold[Stream[IO, Unit]](Stream.raiseError[IO](NotFoundException(traceId, s"${entry.sourceUri} not found")))(m =>
                Stream.eval(metadataCacheAlg.updateCache(entry.localObjectPath, m))
              )
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
  def acquireLock(req: AcquireLockRequest)(implicit ev: Ask[IO, TraceId]): IO[Unit] = {
    val actionToLock = for {
      traceId <- ev.ask[TraceId]
      storageAlg <- storageAlgRef.get
      context <- storageLinksAlg.findStorageLink(req.localObjectPath)
      current <- IO.realTimeInstant
      gsPath = getGsPath(req.localObjectPath, context)
      hashedLockedByCurrentUser <- IO.fromEither(hashString(lockedByString(gsPath.bucketName, config.ownerEmail)))
      newLock = Lock(hashedLockedByCurrentUser, Instant.ofEpochMilli(current.toEpochMilli + config.lockExpiration.toMillis))
      meta <- storageAlg.retrieveAdaptedGcsMetadata(req.localObjectPath, gsPath)
      _ <- meta match {
        case Some(m) =>
          for {
            _ <- metadataCacheAlg.updateRemoteStateCache(req.localObjectPath, RemoteState.Found(m.lock, m.crc32c))
            _ <- m.lock match {
              case Some(lock) =>
                if (lock.hashedLockedBy == hashedLockedByCurrentUser)
                  updateGcsMetadataAndCache(storageAlg, req.localObjectPath, gsPath, newLock).void
                else
                  IO.raiseError(LockedByOther(traceId, s"lock is already acquired by someone else"))
              case None =>
                updateGcsMetadataAndCache(storageAlg, req.localObjectPath, gsPath, newLock).void
            }
          } yield ()
        case None =>
          IO.raiseError(NotFoundException(traceId, s"${gsPath} not found in Google Storage"))
      }
    } yield ()

    preventConcurrentAction(actionToLock, req.localObjectPath)
  }

  private def updateGcsMetadataAndCache(storageAlg: CloudStorageAlg, localPath: RelativePath, gsPath: GsPath, lock: Lock)(
      implicit ev: Ask[IO, TraceId]
  ): IO[Unit] =
    storageAlg
      .updateMetadata(gsPath, lock.toMetadataMap)
      .flatMap(updateMetadataResponse =>
        updateMetadataResponse match {
          case UpdateMetadataResponse.DirectMetadataUpdate =>
            metadataCacheAlg.updateLock(localPath, lock)
          case UpdateMetadataResponse.ReUploadObject(generation, crc32c) =>
            metadataCacheAlg.updateLocalFileStateCache(localPath, RemoteState.Found(Some(lock), crc32c), generation)
        }
      )

  /**
    * In case you're wondering what Kleisli is, Kleisli is a data type that wraps a function (A => F[B]), and commonly used for abstracting away some type of dependency.
    * For instance, in this method, we're abstracting away the need to pass around `traceId` explicitly.
    * For more information about Kleisli, check out https://typelevel.org/cats/datatypes/kleisli.html.
    * Even though you don't see many Kleisli used explicitly in welder, it's actually used extensively, because it's a fundamental data type
    * used by http4s, the web library welder depends on.
    */
  def checkMetadata(req: GetMetadataRequest)(implicit ev: Ask[IO, TraceId]): IO[MetadataResponse] =
    for {
      traceId <- ev.ask[TraceId]
      context <- storageLinksAlg.findStorageLink(req.localObjectPath)
      storageAlg <- storageAlgRef.get
      res <- if (context.isSafeMode)
        IO.pure(MetadataResponse.SafeMode(context.storageLink))
      else {
        val fullBlobName = getFullBlobName(context.basePath, req.localObjectPath.asPath, context.storageLink.cloudStorageDirectory.blobPath)
        val actionToLock = for {
          metadata <- storageAlg
            .retrieveAdaptedGcsMetadata(req.localObjectPath, GsPath(context.storageLink.cloudStorageDirectory.container.asGcsBucket, fullBlobName))
          result <- metadata match {
            case None =>
              metadataCacheAlg.updateRemoteStateCache(req.localObjectPath, RemoteState.NotFound).as(MetadataResponse.RemoteNotFound(context.storageLink))
            case Some(AdaptedGcsMetadata(lock, crc32c, generation)) =>
              val localAbsolutePath = config.workingDirectory.resolve(req.localObjectPath.asPath)
              for {
                calculatedCrc32c <- Crc32c.calculateCrc32ForFile(localAbsolutePath)
                syncStatus <- if (calculatedCrc32c == crc32c)
                  IO.pure(SyncStatus.Live)
                else {
                  for {
                    metaOpt <- metadataCacheAlg.getCache(req.localObjectPath)
                    loggingContext = MetaLoggingContext(traceId, "checkMetadata", context, metaOpt.flatMap(_.localFileGeneration), generation).toMap
                    status <- metaOpt match {
                      case Some(meta) =>
                        meta.localFileGeneration match { //TODO: shall we check the file has been localized before calculating crc32c
                          case Some(previousGeneration) =>
                            if (previousGeneration == generation)
                              IO.pure(SyncStatus.LocalChanged) <* logger.warn(loggingContext)(s"local file has changed, but it hasn't been delocalized yet")
                            else IO.pure(SyncStatus.RemoteChanged) <* logger.warn(loggingContext)("remote has changed")
                          case None =>
                            IO.pure(SyncStatus.Desynchronized) <* logger.error(loggingContext)(
                              "We don't find local generation for a localized file. Was this file localized manually?"
                            )
                        }
                      case None =>
                        IO.pure(SyncStatus.Desynchronized) <* logger.error(loggingContext)(
                          "We don't find local cache for a localized file. Did you update metadata cache file directly?"
                        )
                      //TODO: this shouldn't be possible because we should have the file in cache if it has been localized
                    }
                  } yield status
                }
                _ <- metadataCacheAlg.updateRemoteStateCache(req.localObjectPath, RemoteState.Found(lock, crc32c))
              } yield MetadataResponse.EditMode(syncStatus, lock.map(_.hashedLockedBy), generation, context.storageLink)
          }
        } yield result
        preventConcurrentAction(actionToLock, req.localObjectPath)
      }
    } yield res

  def safeDelocalize(req: SafeDelocalize)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    for {
      traceId <- ev.ask[TraceId]
      context <- storageLinksAlg.findStorageLink(req.localObjectPath)
      _ <- if (context.isSafeMode)
        IO.raiseError(SafeDelocalizeSafeModeFileError(traceId, s"${req.localObjectPath} can't be delocalized since it's in safe mode"))
      else if (context.storageLink.pattern.findFirstIn(req.localObjectPath.asPath.toString).isEmpty)
        logger.info(Map(TRACE_ID_LOGGING_KEY -> traceId.asString))(
          s"ignore ${req.localObjectPath} because it doesn't satisfy ${context.storageLink.pattern} pattern"
        )
      else {
        val localAbsolutePath = config.workingDirectory.resolve(req.localObjectPath.asPath)
        val actionToLock: IO[Unit] = for {
          previousMeta <- metadataCacheAlg.getCache(req.localObjectPath)
          gsPath = getGsPath(req.localObjectPath, context)
          now <- IO.realTimeInstant
          calculatedCrc32c <- Crc32c.calculateCrc32ForFile(localAbsolutePath)

          // If we have lock info in local cache, we check cache to see if we hold a valid lock.
          // local cache should be updated pretty frequently since acquireLock is called much more often than lock expires.
          // Hence, we should be able to rely on cache for checking lock
          _ <- previousMeta match {
            case Some(meta) =>
              meta.remoteState match {
                case RemoteState.NotFound =>
                  delocalizeAndUpdateCache(req.localObjectPath, gsPath, 0L, None)
                case RemoteState.Found(lock, crc32c) =>
                  if (calculatedCrc32c == crc32c)
                    IO.unit
                  else
                    for {
                      _ <- lock.traverse(l => checkLock(l, now.toEpochMilli, gsPath.bucketName))
                      //here, generation should always exist, but there's no risk in setting it to 0L since delocalize will be rejected by GSC if the file actually exist in GCS
                      //push previously existing lock if it exists so that we don't overwrite remote lock when we delocalize file
                      _ <- delocalizeAndUpdateCache(req.localObjectPath, gsPath, meta.localFileGeneration.getOrElse(0L), lock)
                    } yield ()
              }
            case None =>
              // If this is a new file, acquire lock for current user; If the file actually exists in GCE, delocalize will fail with generation mismatch.
              for {
                hashedLockedByCurrentUser <- IO.fromEither(hashString(lockedByString(gsPath.bucketName, config.ownerEmail)))
                current <- IO.realTimeInstant
                lock = Lock(hashedLockedByCurrentUser, Instant.ofEpochMilli(current.toEpochMilli + config.lockExpiration.toMillis))
                _ <- delocalizeAndUpdateCache(req.localObjectPath, gsPath, 0L, Some(lock))
              } yield ()
          }
        } yield ()

        preventConcurrentAction(actionToLock, req.localObjectPath)
      }
    } yield ()

  def delete(req: Delete)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    for {
      traceId <- ev.ask[TraceId]
      context <- storageLinksAlg.findStorageLink(req.localObjectPath)
      _ <- if (context.isSafeMode)
        IO.raiseError(DeleteSafeModeFileError(traceId, s"${req.localObjectPath} can't be deleted since it's in safe mode"))
      else {
        val gsPath = getGsPath(req.localObjectPath, context)
        val actionToLock = for {
          previousMeta <- metadataCacheAlg.getCache(req.localObjectPath)
          now <- IO.realTimeInstant
          generation <- previousMeta match {
            case Some(meta) =>
              meta.remoteState match {
                case RemoteState.NotFound => IO.pure(0L)
                case RemoteState.Found(lock, _) =>
                  lock
                    .traverse(l => checkLock(l, now.toEpochMilli, gsPath.bucketName)) //check if user owns the lock before deleting
                    .as(meta.localFileGeneration.getOrElse(0L))
              }
            case None =>
              IO.raiseError(InvalidLock(traceId, s"Local GCS metadata for ${req.localObjectPath} not found"))
          }
          storageAlg <- storageAlgRef.get
          _ <- storageAlg.removeObject(gsPath, Some(generation)).compile.drain.void
          _ <- metadataCacheAlg.removeCache(req.localObjectPath) // remove the object from cache
        } yield ()
        preventConcurrentAction(actionToLock, req.localObjectPath)
      }
    } yield ()

  private def delocalizeAndUpdateCache(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, lock: Option[Lock])(
      implicit ev: Ask[IO, TraceId]
  ): IO[Unit] = {
    val lockMetadataToPush = lock.map(_.toMetadataMap).getOrElse(Map.empty)
    for {
      storageAlg <- storageAlgRef.get
      delocalizeResp <- storageAlg.delocalize(localObjectPath, gsPath, generation, lockMetadataToPush).recoverWith {
        case e: com.google.cloud.storage.StorageException if e.getCode == 412 =>
          if (generation == 0L)
            IO.raiseError(e)
          else {
            // In the case when the file is already been deleted from GCS, we try to delocalize the file with generation being 0L
            // This assumes the business logic we want is always to recreate files that have been deleted from GCS by other users.
            // If the file is indeed out of sync with remote, both delocalize attempts will fail due to generation mismatch
            storageAlg.delocalize(localObjectPath, gsPath, 0L, lockMetadataToPush)
          }
      }
      _ <- delocalizeResp.traverse(r => metadataCacheAlg.updateLocalFileStateCache(localObjectPath, RemoteState.Found(lock, r.crc32c), r.generation))
    } yield ()
  }

  /**
    * If lock exists and it hasn't expired, we return IO[Unit]; Otherwise, raise error
    */
  private def checkLock(lock: Lock, now: Long, bucketName: GcsBucketName)(implicit ev: Ask[IO, TraceId]): IO[Unit] =
    for {
      traceId <- ev.ask[TraceId]
      hashedLockedByCurrentUser <- IO.fromEither(hashString(lockedByString(bucketName, config.ownerEmail)))
      _ <- IO
        .raiseError(
          InvalidLock(
            traceId,
            s"Fail to delocalize due to lock held by someone else. Lock held by ${lock.hashedLockedBy} but current user is ${hashedLockedByCurrentUser}."
          )
        )
        .whenA(lock.hashedLockedBy != hashedLockedByCurrentUser)
      _ <- IO
        .raiseError(
          InvalidLock(
            traceId,
            s"Fail to delocalize due to lock expiration. Lock expires at ${lock.lockExpiresAt.toEpochMilli}, and current time is ${now}."
          )
        )
        .whenA(now > lock.lockExpiresAt.toEpochMilli)
    } yield ()

  private[server] def preventConcurrentAction[A](ioa: IO[A], localPath: RelativePath): IO[A] =
    for {
      permitOpt <- permitsRef.get.map(_.get(localPath))
      permit <- permitOpt.fold {
        Semaphore[IO](1L).flatMap(s => permitsRef.modify(mp => (mp + (localPath -> s), s)))
      }(p => IO.pure(p))
      lock = Resource.make[IO, Unit](permit.acquire)(_ => permit.release)
      res <- lock.use(_ => ioa)
    } yield res
}

object ObjectService {
  def apply(
      permitsRef: Permits,
      config: ObjectServiceConfig,
      storageAlgRef: Ref[IO, CloudStorageAlg],
      storageLinksAlg: StorageLinksAlg,
      metadataCacheAlg: MetadataCacheAlg
  )(
      implicit logger: StructuredLogger[IO]
  ): ObjectService = new ObjectService(permitsRef, config, storageAlgRef, storageLinksAlg, metadataCacheAlg)

  implicit val actionDecoder: Decoder[Action] = Decoder.decodeString.emap(str => Action.stringToAction.get(str).toRight("invalid action"))

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
    extends LoggingContext {
  override def toMap: Map[String, String] =
    Map(
      "traceId" -> traceId.asString,
      "action" -> action,
      "oldGeneration" -> oldGeneration.getOrElse(-1).toString,
      "newGeneration" -> newGeneration.toString
    ) ++ commonContext.toMap
}
