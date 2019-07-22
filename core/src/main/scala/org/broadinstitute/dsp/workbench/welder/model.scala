package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path
import java.time.Instant

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

sealed abstract class SyncStatus extends Product with Serializable
object SyncStatus {
  // crc32c match
  final case object Live extends SyncStatus {
    override def toString: String = "LIVE"
  }
  // crc32c mismatch
  final case object RemoteChanged extends SyncStatus {
    override def toString: String = "REMOTE_CHANGED"
  }
  final case object LocalChanged extends SyncStatus {
    override def toString: String = "LOCAL_CHANGED"
  }
  final case object Desynchronized extends SyncStatus {
    override def toString: String = "DESYNCHRONIZED"
  }
  // deleted in gcs. (object exists in storagelinks config file but not in in gcs)
  final case object RemoteNotFound extends SyncStatus {
    override def toString: String = "REMOTE_NOT_FOUND"
  }

  val stringToSyncStatus: Set[SyncStatus] = sealerate.values[SyncStatus]
}

sealed abstract class SyncMode extends Product with Serializable
object SyncMode {
  final case object Safe extends SyncMode {
    override def toString: String = "SAFE"
  }
  final case object Edit extends SyncMode {
    override def toString: String = "EDIT"
  }
}

final case class BlobPath(asString: String) extends AnyVal

sealed abstract class SourceUri
object SourceUri {
  final case class DataUri(data: Array[Byte]) extends SourceUri
  final case class GsPath(bucketName: GcsBucketName, blobName: GcsBlobName) extends SourceUri {
    override def toString: String = s"gs://${bucketName.value}/${blobName.value}"
  }
}

sealed abstract class LocalDirectory {
  def path: RelativePath
}
object LocalDirectory {
  final case class LocalBaseDirectory(path: RelativePath) extends LocalDirectory
  final case class LocalSafeBaseDirectory(path: RelativePath) extends LocalDirectory
}

final case class CloudStorageDirectory(bucketName: GcsBucketName, blobPath: Option[BlobPath])

final case class StorageLink(
    localBaseDirectory: LocalDirectory,
    localSafeModeBaseDirectory: LocalDirectory,
    cloudStorageDirectory: CloudStorageDirectory,
    pattern: String
)

final case class HashedLockedBy(asString: String) extends AnyVal

/**
  * Data type represents a lock that hasn't expired
  * @param hashedLockedBy hash of who owns the lock welder knows about most recently
  * @param lockExpiresAt Instant of when lock expires
  */
final case class Lock(hashedLockedBy: HashedLockedBy, lockExpiresAt: Instant) {
  def toMetadataMap: Map[String, String] = Map(
    LAST_LOCKED_BY -> hashedLockedBy.asString,
    LOCK_EXPIRES_AT -> lockExpiresAt.toEpochMilli.toString
  )
}

// This case class doesn't mirror exactly metadata from GCS, we adapted raw metadata from GCS and only keep fields we care
/**
  * @param lock lock related info; This should get updated every time welder interacts with Google
  * @param crc32c previous hash of a local file when it gets localized; We don't update this value every time we interact with Google because we need to know local file's crc32c when it was pulled from GCS
  * @param generation previous generation of a local file when it gets localized; We don't update this value every time we interact with Google because we need to know local file's crc32c when it was pulled from GCS
  */
final case class AdaptedGcsMetadata(lock: Option[Lock], crc32c: Crc32, generation: Long)

/**
  * @param lock lock related info. lockExpiresAt is only populated when lock is held by current user; This should get updated every time welder interacts with Google
  * @param crc32c latest crc32c we know in GCS
  */
final case class RemoteState(lock: Option[Lock], crc32c: Crc32)

/**
  * @param localPath local relative path to a file
  * @param remoteState File state in GCS as far as Welder is aware. Updated every time we interacts with GCS
  * @param localFileGeneration Some() when a file is localized or delocalized; None when the file has not been localized
  */
final case class AdaptedGcsMetadataCache(localPath: RelativePath, remoteState: RemoteState, localFileGeneration: Option[Long])

final case class RelativePath(asPath: Path) extends AnyVal {
  override def toString: String = asPath.toString
}
