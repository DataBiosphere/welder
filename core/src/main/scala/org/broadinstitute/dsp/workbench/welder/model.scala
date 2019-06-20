package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
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
  def path: Path
}
object LocalDirectory {
  final case class LocalBaseDirectory(path: Path) extends LocalDirectory
  final case class LocalSafeBaseDirectory(path: Path) extends LocalDirectory
}

final case class CloudStorageDirectory(bucketName: GcsBucketName, blobPath: BlobPath)

final case class StorageLink(localBaseDirectory: LocalDirectory, localSafeModeBaseDirectory: LocalDirectory, cloudStorageDirectory: CloudStorageDirectory, pattern: String)

// This case class doesn't mirror exactly metadata from GCS, we adapted raw metadata from GCS and only keep fields we care
final case class AdaptedGcsMetadata(localPath: RelativePath, lastLockedBy: Option[WorkbenchEmail], crc32c: Crc32, generation: Long)

final case class RelativePath(asPath: Path) extends AnyVal {
  override def toString: String = asPath.toString
}