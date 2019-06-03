package org.broadinstitute.dsp.workbench

import java.nio.file.Path
import java.util.Base64

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
  def parseGsPath(str: String): Either[String, GsPath] = for {
    _ <- gsDirectoryReg.findPrefixOf(str).toRight("gs directory has to be prefixed with gs://")
    parsed <- Either.catchNonFatal(str.split("/")).leftMap(_.getMessage)
    bucketName <- Either.catchNonFatal(parsed(2))
      .leftMap(_ => s"failed to parse bucket name")
      .ensure("bucketName can't be empty")(s => s.nonEmpty)
    objectName <- Either.catchNonFatal(parsed.drop(3).mkString("/"))
      .leftMap(_ => s"failed to parse object name")
      .ensure("objectName can't be empty")(s => s.nonEmpty)
  } yield GsPath(GcsBucketName(bucketName), GcsBlobName(objectName))

  // base directory example: “workspaces/ws1”
  def getLocalBaseDirectory(localPath: Path): Either[String, Path] = {
    for {
      prefix <- Either.catchNonFatal(localPath.getName(0)).leftMap(_ => s"no valid prefix found for $localPath")
      workspaceName <- Either.catchNonFatal(localPath.getName(1)).leftMap(_ => s"no workspace name found for $localPath")
    } yield localPath.subpath(0, 2)
  }

  def getFullBlobName(localPath: Path, blobPath: BlobPath): Either[String, GcsBlobName] =
    for {
      prefix <- getLocalBaseDirectory(localPath)
      subPath = prefix.relativize(localPath)
    } yield GcsBlobName(blobPath.asString + "/" + subPath.toString)

  val base64Decoder = Base64.getDecoder()
  def base64DecoderPipe[F[_]: Sync]: Pipe[F, String, Byte] = in => {
    in.evalMap(s => Sync[F].catchNonFatal(base64Decoder.decode(s)))
      .flatMap(bytes => Stream.emits(bytes).covary[F])
  }

  type StorageLinksCache = Ref[IO, Map[LocalBasePath, StorageLink]]
  type MetadataCache = Ref[IO, Map[Path, GcsMetadata]]

  val gcpObjectType = "text/plain"
}
