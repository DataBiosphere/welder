package org.broadinstitute.dsp.workbench.welder

import cats.Eq
import cats.effect.{ContextShift, IO, Timer}
import io.chrisdavenport.linebacker.Linebacker
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.Matchers
import org.scalatest.prop.Configuration
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global

trait WelderTestSuite extends Matchers with ScalaCheckPropertyChecks with Configuration {
  implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)
  implicit val timer: Timer[IO] = IO.timer(executionContext)
  implicit val lineBacker = Linebacker.fromExecutionContext[IO](global)
  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 3)

  // Regex's equals doesn't compare Regex as expected. Hence, we define Eq[StorageLink] when we need to check equality of two StorageLink
  implicit val storageLinkEq: Eq[StorageLink] = Eq.instance{ (s1, s2) =>
    s1.pattern.pattern.pattern == s2.pattern.pattern.pattern &&
    s1.localBaseDirectory == s2.localBaseDirectory &&
    s1.localSafeModeBaseDirectory == s2.localSafeModeBaseDirectory &&
    s1.cloudStorageDirectory == s2.cloudStorageDirectory
  }
}

