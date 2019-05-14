package org.broadinstitute.dsp.workbench.welder.server

import cats.effect.IO
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl

object StorageLinksService extends Http4sDsl[IO] {
  val service: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root =>
      Ok(getStorageLinks())
    case PUT -> Root =>
      Ok(updateStorageLinks)
    case POST -> Root =>
      Ok(createStorageLinks())
  }

  def createStorageLinks(): IO[Unit] = ???

  def updateStorageLinks(): IO[Unit] = ???

  def getStorageLinks(): IO[Unit] = ???
}

final case class LocalDirectory(asString: String) extends AnyVal
final case class GsDirectory(asString: String) extends AnyVal
final case class StorageLinks(localBaseDirectory: LocalDirectory, cloudStorageDirectory: GsDirectory, pattern: String, recursive: Boolean)
