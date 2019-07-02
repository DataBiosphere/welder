package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO

trait StorageLinksAlg {
  def findStorageLink[A](localPath: RelativePath): IO[CommonContext]
}

object StorageLinksAlg {
  def fromCache(storageLinksCache: StorageLinksCache): StorageLinksAlg = new StorageLinksInterp(storageLinksCache)
}
final case class CommonContext(isSafeMode: Boolean, basePath: RelativePath, storageLink: StorageLink)
