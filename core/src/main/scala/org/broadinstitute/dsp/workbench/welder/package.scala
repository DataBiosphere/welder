package org.broadinstitute.dsp.workbench

import java.nio.file.Path

import cats.implicits._
import io.circe.fs2._
import fs2.Stream
import cats.effect.{Concurrent, ContextShift, Sync}
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
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

  def parseGsDirectory(str: String): Either[String, BucketNameAndObjectName] = for {
    parsed <- Either.catchNonFatal(str.split("/")).leftMap(_.getMessage)
    bucketName <- Either.catchNonFatal(parsed(2))
      .leftMap(_ => s"failed to parse bucket name")
      .ensure("bucketName can't be empty")(s => s.nonEmpty)
    objectName <- Either.catchNonFatal(parsed.drop(3).mkString("/"))
      .leftMap(_ => s"failed to parse object name")
      .ensure("objectName can't be empty")(s => s.nonEmpty)
  } yield BucketNameAndObjectName(GcsBucketName(bucketName), GcsBlobName(objectName))
}
