package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.TraceId

trait StorageLinksAlg {
  def findStorageLink[A](localPath: RelativePath)(implicit ev: Ask[IO, TraceId]): IO[CommonContext]
}

object StorageLinksAlg {
  def fromCache(storageLinksCache: StorageLinksCache): StorageLinksAlg = new StorageLinksInterp(storageLinksCache)
}
final case class CommonContext(isSafeMode: Boolean, basePath: RelativePath, storageLink: StorageLink) extends LoggingContext {
  override def toMap: Map[String, String] = {
    val safeModeBaseDirectory =
      storageLink.localSafeModeBaseDirectory.fold[Map[String, String]](Map.empty)(l => Map("storageLink.localSafeModeBaseDirectory" -> l.path.toString))
    Map(
      "isSafeMode" -> isSafeMode.toString,
      "basePath" -> basePath.asPath.toString,
      "storageLink.localBaseDirectory" -> storageLink.localBaseDirectory.path.toString,
      "storageLink.cloudStorageDirectory" -> storageLink.cloudStorageDirectory.toString,
      "storageLink.pattern" -> storageLink.pattern.toString
    ) ++ safeModeBaseDirectory
  }
}
