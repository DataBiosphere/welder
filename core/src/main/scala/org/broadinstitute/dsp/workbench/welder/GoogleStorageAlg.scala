package org.broadinstitute.dsp.workbench.welder

import cats.effect.{ContextShift, IO, Timer}
import fs2.Stream
import io.chrisdavenport.linebacker.Linebacker
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath

trait GoogleStorageAlg {
  def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[Unit]
  def retrieveGcsMetadata(localPath: RelativePath, bucketName: GcsBucketName, blobName: GcsBlobName, traceId: TraceId): IO[Option[GcsMetadata]]
  def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult]
  def gcsToLocalFile(localAbsolutePath: java.nio.file.Path, gcsBucketName: GcsBucketName, gcsBlobName: GcsBlobName): Stream[IO, Unit]
  def delocalize(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, traceId: TraceId): IO[Unit]
}

object GoogleStorageAlg {
  val LAST_LOCKED_BY = "lastLockedBy"
  val LOCK_EXPIRES_AT = "lockExpiresAt"

  def fromGoogle(config: GoogleStorageAlgConfig , googleStorageService: GoogleStorageService[IO])
                (implicit logger: Logger[IO], timer: Timer[IO], linerBacker: Linebacker[IO], cs: ContextShift[IO]): GoogleStorageAlg = new GoogleStorageInterp(config, googleStorageService)
}

final case class GoogleStorageAlgConfig(workingDirectory: java.nio.file.Path)