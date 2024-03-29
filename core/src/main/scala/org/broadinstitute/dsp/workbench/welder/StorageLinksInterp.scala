package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.TraceId

class StorageLinksInterp(storageLinksCache: StorageLinksCache) extends StorageLinksAlg {
  def findStorageLink[A](localPath: RelativePath)(implicit ev: Ask[IO, TraceId]): IO[CommonContext] =
    for {
      traceId <- ev.ask[TraceId]
      storageLinks <- storageLinksCache.get
      baseDirectories = getPossibleBaseDirectory(localPath.asPath)
      context = baseDirectories.collectFirst {
        case x if (storageLinks.get(RelativePath(x)).isDefined) =>
          val relativePath = RelativePath(x)
          val sl = storageLinks.get(relativePath).get
          val isSafeMode = sl.localSafeModeBaseDirectory.exists(_.path == relativePath)
          CommonContext(isSafeMode, relativePath, sl)
      }
      res <- context.fold[IO[CommonContext]](IO.raiseError(StorageLinkNotFoundException(traceId, s"No storage link found for ${localPath}")))(IO.pure)
    } yield res
}
