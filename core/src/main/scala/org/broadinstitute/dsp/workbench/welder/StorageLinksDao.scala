package org.broadinstitute.dsp.workbench.welder

import java.nio.file.Path

import cats.effect.IO

class StorageLinksDao(storageLinksCache: StorageLinksCache) {
  def findStorageLink[A](localPath: RelativePath): IO[CommonContext] = for {
    storageLinks <- storageLinksCache.get
    baseDirectories = getPosssibleBaseDirectory(localPath.asPath)
    context = baseDirectories.collectFirst {
      case x if (storageLinks.get(x).isDefined) =>
        val sl = storageLinks.get(x).get
        val isSafeMode = sl.localSafeModeBaseDirectory.path == x
        CommonContext(isSafeMode, x, sl)
    }
    res <- context.fold[IO[CommonContext]](IO.raiseError(StorageLinkNotFoundException(s"No storage link found for ${localPath}")))(IO.pure)
  } yield res
}

final case class CommonContext(isSafeMode: Boolean, basePath: Path, storageLink: StorageLink)
