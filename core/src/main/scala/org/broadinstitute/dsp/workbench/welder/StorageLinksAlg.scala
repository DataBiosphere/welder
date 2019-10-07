package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.model.TraceId

trait StorageLinksAlg {
  def findStorageLink[A](localPath: RelativePath)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[CommonContext]
}

object StorageLinksAlg {
  def fromCache(storageLinksCache: StorageLinksCache): StorageLinksAlg = new StorageLinksInterp(storageLinksCache)
}
final case class CommonContext(isSafeMode: Boolean, basePath: RelativePath, storageLink: StorageLink)
