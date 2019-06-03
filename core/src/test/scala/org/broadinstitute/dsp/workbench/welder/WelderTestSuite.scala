package org.broadinstitute.dsp.workbench.welder

import cats.effect.{ContextShift, IO, Timer}
import org.scalatest.Matchers
import org.scalatest.prop.Configuration
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext

trait WelderTestSuite extends Matchers with ScalaCheckPropertyChecks with Configuration {
  implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)
  implicit val timer: Timer[IO] = IO.timer(executionContext)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 3)
}

