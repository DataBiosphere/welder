package org.broadinstitute.dsp.workbench.welder
package server

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

import cats.effect.{IO, Timer}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.broadinstitute.dsp.workbench.welder.server.LockService._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.dsl.Http4sDsl

import scala.concurrent.duration.FiniteDuration

class LockService(config: LockServiceConfig,
                   storageLinksDao: StorageLinksDao,
                   googleStorageService: GoogleStorageService[IO])(implicit timer: Timer[IO], logger: Logger[IO]) extends Http4sDsl[IO] {

  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ POST -> Root => for {
      traceId <- IO(TraceId(randomUUID()))
      request <- req.as[AcquireLockRequest]
      _ <- acquireLock(request, traceId)
      resp <- NoContent()
    } yield resp
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
  def acquireLock(req: AcquireLockRequest, traceId: TraceId): IO[Unit] = {
    for {
      context <- storageLinksDao.findStorageLink(req.localObjectPath)
      current <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      metadata = Map(
        "lastLockedBy" -> config.ownerEmail.value,
        "lockExpiresAts" -> (current + config.lockExpiresIn.toMillis).toString)
      gsPath = getGsPath(req.localObjectPath, context)
      _ <- googleStorageService.setObjectMetadata(gsPath.bucketName, gsPath.blobName, metadata, Option(traceId)).compile.drain.handleErrorWith {
        case e: com.google.cloud.storage.StorageException if(e.getCode == 403) =>
          for {
            _ <- logger.info(s"$traceId | Fail to update lock due to 403. Going to download the blob and re-upload")
            bytes <- googleStorageService.getObject(gsPath.bucketName, gsPath.blobName, Some(traceId)).compile.to[Array]
            _ <- googleStorageService.storeObject(gsPath.bucketName, gsPath.blobName, bytes, "text/plain", metadata, None, Some(traceId)).compile.drain
          } yield ()
      }
    } yield ()
  }
}

object LockService {
  def apply(config: LockServiceConfig,
            storageLinksDao: StorageLinksDao,
            googleStorageService: GoogleStorageService[IO])(implicit timer: Timer[IO]): LockService = new LockService(config, storageLinksDao, googleStorageService)

  implicit val acquireLockRequestDecoder: Decoder[AcquireLockRequest] = Decoder.forProduct1("localObjectPath")(AcquireLockRequest.apply)
}

final case class AcquireLockRequest(localObjectPath: RelativePath)

final case class LockServiceConfig(ownerEmail: WorkbenchEmail, lockExpiresIn: FiniteDuration)