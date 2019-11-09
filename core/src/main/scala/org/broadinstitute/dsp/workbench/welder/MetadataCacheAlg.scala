package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path

import cats.effect.IO
import fs2.Pipe

trait MetadataCacheAlg {
  def updateRemoteStateCache(localPath: RelativePath, remoteState: RemoteState): IO[Unit]

  def updateLocalFileStateCache(localPath: RelativePath, remoteState: RemoteState, localFileGeneration: Long): IO[Unit]

  def updateCache(localPath: RelativePath, adaptedGcsMetadata: AdaptedGcsMetadata): IO[Unit]

  def updateLock(localPath: RelativePath, lock: Lock): IO[Unit]

  def getCache(localPath: RelativePath): IO[Option[AdaptedGcsMetadataCache]]

  def removeCache(localPath: RelativePath): IO[Unit]

  def updateCachePipe: Pipe[IO, AdaptedGcsMetadataCache, Unit]
}

final case class CacheAlgConfig(storageLinksPath: Path, metadataCachePath: Path)
