package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import fs2.{Pipe, Stream}

class MetadataCacheInterp(metadataCache: MetadataCache) extends MetadataCacheAlg {
  def updateRemoteStateCache(localPath: RelativePath, remoteState: RemoteState): IO[Unit] =
    metadataCache.modify { mp =>
      val previousMeta = mp.get(localPath)
      val newCache = mp + (localPath -> AdaptedGcsMetadataCache(localPath, remoteState, previousMeta.flatMap(_.localFileGeneration)))
      (newCache, ())
    }

  def updateLocalFileStateCache(localPath: RelativePath, remoteState: RemoteState, localFileGeneration: Long): IO[Unit] =
    metadataCache.modify { mp =>
      val previousMeta = mp.get(localPath)
      val newCache = mp + (localPath -> AdaptedGcsMetadataCache(localPath, remoteState, Some(localFileGeneration)))
      (newCache, ())
    }

  def updateCache(localPath: RelativePath, adaptedGcsMetadata: AdaptedGcsMetadata): IO[Unit] =
    metadataCache.modify { mp =>
      val newCache = mp + (localPath -> AdaptedGcsMetadataCache(
        localPath,
        RemoteState(adaptedGcsMetadata.lock, adaptedGcsMetadata.crc32c),
        Some(adaptedGcsMetadata.generation)
      ))
      (newCache, ())
    }

  val updateCachePipe: Pipe[IO, AdaptedGcsMetadataCache, Unit] = in => {
    in.flatMap { metadata =>
      val res = metadataCache.modify { mp =>
        val newCache = mp + (metadata.localPath -> metadata)
        (newCache, ())
      }
      Stream.eval(res)
    }
  }

  override def getCache(localPath: RelativePath): IO[Option[AdaptedGcsMetadataCache]] = metadataCache.get.map(_.get(localPath))

  override def removeCache(localPath: RelativePath): IO[Unit] = metadataCache.modify(mp => (mp - localPath, ()))
}
