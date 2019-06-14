package org.broadinstitute.dsp.workbench

import java.nio.file.Path
import java.util.Base64

import cats.Eq
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, IO, Sync}
import cats.implicits._
import fs2.{Pipe, Stream}
import io.circe.Decoder
import io.circe.fs2._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
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

  val gsDirectoryReg = "gs:\\/\\/.*".r

  def validateGsPrefix(str: String): Either[String, Unit] = gsDirectoryReg.findPrefixOf(str).void.toRight("gs directory has to be prefixed with gs://")

  def parseBucketName(str: String): Either[String, GcsBucketName] = for {
    _ <- validateGsPrefix(str)
    parsed <- Either.catchNonFatal(str.split("/")).leftMap(_.getMessage)
    bucketName <- Either.catchNonFatal(parsed(2))
      .leftMap(_ => s"failed to parse bucket name")
      .ensure("bucketName can't be empty")(s => s.nonEmpty)
  } yield GcsBucketName(bucketName)

  def parseGsPath(str: String): Either[String, GsPath] = for {
    bucketName <- parseBucketName(str)
    length = s"gs://${bucketName.value}/".length
    objectName <- Either.catchNonFatal(str.drop(length))
      .leftMap(_ => s"failed to parse object name")
      .ensure("objectName can't be empty")(s => s.nonEmpty)
  } yield GsPath(bucketName, GcsBlobName(objectName))

  def getPosssibleBaseDirectory(localPath: Path): List[Path] = {
    ((localPath.getNameCount - 1).to(1, -1)).map(
      index => localPath.subpath(0, index)
    ).toList
  }

  def getFullBlobName(basePath: Path, localPath: Path, blobPath: BlobPath): GcsBlobName = {
      val subPath = basePath.relativize(localPath)
      GcsBlobName(blobPath.asString + "/" + subPath.toString)
    }

  val base64Decoder = Base64.getDecoder()
  def base64DecoderPipe[F[_]: Sync]: Pipe[F, String, Byte] = in => {
    in.evalMap(s => Sync[F].catchNonFatal(base64Decoder.decode(s)))
      .flatMap(bytes => Stream.emits(bytes).covary[F])
  }

  type StorageLinksCache = Ref[IO, Map[Path, StorageLink]]
  type MetadataCache = Ref[IO, Map[Path, GcsMetadata]]

  val gcpObjectType = "text/plain"

  implicit val eqLocalDirectory: Eq[LocalDirectory] = Eq.instance((p1, p2) => p1.path.toString == p2.path.toString)
}
