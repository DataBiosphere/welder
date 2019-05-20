package org.broadinstitute.dsp.workbench

import java.nio.file.Path
import io.circe.fs2._
import fs2.Stream
import cats.effect.{Concurrent, ContextShift, Sync}
import io.circe.Decoder
import org.typelevel.jawn.AsyncParser

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global

package object welder {
  def readJsonFileToA[F[_]: Sync: ContextShift: Concurrent, A: Decoder](path: Path, blockingExecutionContext: Option[ExecutionContext] = None): Stream[F, A] = {
    fs2.io.file.readAll[F](path, blockingExecutionContext.getOrElse(global), 4096)
      .through(fs2.text.utf8Decode)
      .through(_root_.io.circe.fs2.stringParser(AsyncParser.SingleValue))
      .through(decoder)
  }
}
