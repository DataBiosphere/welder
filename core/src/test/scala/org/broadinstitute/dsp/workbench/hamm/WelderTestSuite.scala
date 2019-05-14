package org.broadinstitute.dsp.workbench.hamm

import cats.effect.{ContextShift, IO}
import org.scalatest.Matchers
import scala.concurrent.ExecutionContext

trait WelderTestSuite extends Matchers {
  implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)
}

