package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO

class StorageLinksInterp(storageLinksCache: StorageLinksCache) extends StorageLinksAlg {
  def findStorageLink[A](localPath: RelativePath): IO[CommonContext] = for {
    storageLinks <- storageLinksCache.get
    baseDirectories = getPossibleBaseDirectory(localPath.asPath)
    context = baseDirectories.collectFirst {
      case x if (storageLinks.get(x).isDefined) =>
        val sl = storageLinks.get(x).get
        val isSafeMode = sl.localSafeModeBaseDirectory.path == x
        CommonContext(isSafeMode, x, sl)
    }
    res <- context.fold[IO[CommonContext]](IO.raiseError(StorageLinkNotFoundException(s"No storage link found for ${localPath}")))(IO.pure)
  } yield res
}
