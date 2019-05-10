package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect._
import org.http4s.HttpApp
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.http4s.server.middleware.Logger
import org.http4s.syntax.kleisli._

class WelderApp(implicit cs: ContextShift[IO]) extends Http4sDsl[IO] {
  val routes: HttpApp[IO] = Router[IO] (
    "/status" -> StatusService.service,
    "/storageLinks" -> StorageLinksService.service
  ).orNotFound

  val service: HttpApp[IO] = Logger.httpApp(true, true)(routes)
}

object WelderApp {
  def apply(implicit cs: ContextShift[IO]): WelderApp = new WelderApp()
}