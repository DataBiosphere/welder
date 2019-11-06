package org.broadinstitute.dsp.workbench.welder

import cats.Eq
import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.StructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.model.TraceId
import org.http4s.{Header, Headers}
import org.scalatest.Matchers
import org.scalatest.prop.Configuration
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global
import scala.util.matching.Regex

trait WelderTestSuite extends Matchers with ScalaCheckPropertyChecks with Configuration {
  implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)
  implicit val timer: Timer[IO] = IO.timer(executionContext)
  implicit val unsafeLogger: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val fakeTraceId = TraceId("fakeTraceId")
  val fakeTraceIdHeader = Headers.of(Header("X-Cloud-Trace-Context", fakeTraceId.asString))
  implicit val traceId: ApplicativeAsk[IO, TraceId] = ApplicativeAsk.const[IO, TraceId](fakeTraceId)

  val blocker: Blocker = Blocker.liftExecutionContext(global)
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 3)

  // Regex's equals doesn't compare Regex as expected. Hence, we define Eq[StorageLink] when we need to check equality of two StorageLink
  implicit val regexEq: Eq[Regex] = Eq.instance((r1, r2) => r1.pattern.pattern == r2.pattern.pattern)

  implicit val storageLinkEq: Eq[StorageLink] = Eq.instance{ (s1, s2) =>
    s1.pattern.pattern.pattern == s2.pattern.pattern.pattern &&
    s1.localBaseDirectory == s2.localBaseDirectory &&
    s1.localSafeModeBaseDirectory == s2.localSafeModeBaseDirectory &&
    s1.cloudStorageDirectory == s2.cloudStorageDirectory
  }
}

