package org.broadinstitute.dsp.workbench

import java.math.BigInteger
import java.nio.file.Path
import java.security.MessageDigest
import java.util.Base64

import cats.Eq
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, IO, Resource, Sync}
import cats.implicits._
import fs2.{Pipe, Stream}
import io.chrisdavenport.log4cats.Logger
import io.circe.{Decoder, Encoder, Printer}
import io.circe.fs2._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.typelevel.jawn.AsyncParser
import io.circe.syntax._
import scala.language.implicitConversions
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global

package object welder {
  def readJsonFileToA[F[_]: Sync: ContextShift: Concurrent, A: Decoder](path: Path, blockingExecutionContext: Option[ExecutionContext] = None): Stream[F, A] =
    fs2.io.file
      .readAll[F](path, blockingExecutionContext.getOrElse(global), 4096)
      .through(fs2.text.utf8Decode)
      .through(_root_.io.circe.fs2.stringParser(AsyncParser.SingleValue))
      .through(decoder)

  val gsDirectoryReg = "gs:\\/\\/.*".r

  def validateGsPrefix(str: String): Either[String, Unit] = gsDirectoryReg.findPrefixOf(str).void.toRight("gs directory has to be prefixed with gs://")

  def parseBucketName(str: String): Either[String, GcsBucketName] =
    for {
      _ <- validateGsPrefix(str)
      parsed <- Either.catchNonFatal(str.split("/")).leftMap(_.getMessage)
      bucketName <- Either
        .catchNonFatal(parsed(2))
        .leftMap(_ => s"failed to parse bucket name")
        .ensure("bucketName can't be empty")(s => s.nonEmpty)
    } yield GcsBucketName(bucketName)

  def parseGsPath(str: String): Either[String, GsPath] =
    for {
      bucketName <- parseBucketName(str)
      length = s"gs://${bucketName.value}/".length
      objectName <- Either
        .catchNonFatal(str.drop(length))
        .leftMap(_ => s"failed to parse object name")
        .ensure("objectName can't be empty")(s => s.nonEmpty)
    } yield GsPath(bucketName, GcsBlobName(objectName))

  def getPossibleBaseDirectory(localPath: Path): List[Path] =
    ((localPath.getNameCount - 1)
      .to(1, -1))
      .map(
        index => localPath.subpath(0, index)
      )
      .toList

  def getFullBlobName(basePath: RelativePath, localPath: Path, blobPath: Option[BlobPath]): GcsBlobName = {
    val subPath = basePath.asPath.relativize(localPath)
    blobPath match {
      case Some(bp) => GcsBlobName(bp.asString + "/" + subPath.toString)
      case None => GcsBlobName(subPath.toString)
    }
  }

  val base64Decoder = Base64.getDecoder()
  def base64DecoderPipe[F[_]: Sync]: Pipe[F, String, Byte] = in => {
    in.evalMap(s => Sync[F].catchNonFatal(base64Decoder.decode(s)))
      .flatMap(bytes => Stream.emits(bytes).covary[F])
  }

  def getGsPath(localObjectPath: RelativePath, basePathAndStorageLink: CommonContext): GsPath = {
    val fullBlobName =
      getFullBlobName(basePathAndStorageLink.basePath, localObjectPath.asPath, basePathAndStorageLink.storageLink.cloudStorageDirectory.blobPath)
    GsPath(basePathAndStorageLink.storageLink.cloudStorageDirectory.bucketName, fullBlobName)
  }

  //Note that bucketName below does NOT include the gs:// prefix
  //This string will be hashed when stored in GCS metadata
  def lockedByString(bucketName: GcsBucketName, ownerEmail: WorkbenchEmail): String = bucketName.value + ":" + ownerEmail.value

  def hashString(metadata: String): Either[Throwable, HashedLockedBy] = Either.catchNonFatal {
    HashedLockedBy(String.format("%032x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(metadata.getBytes("UTF-8")))))
  }

  type StorageLinksCache = Ref[IO, Map[RelativePath, StorageLink]]
  type MetadataCache = Ref[IO, Map[RelativePath, AdaptedGcsMetadataCache]]

  val gcpObjectType = "text/plain"

  def cachedResource[A, B: Decoder: Encoder](path: Path, blockingEc: ExecutionContext, toTuple: B => List[(A, B)])(
      implicit logger: Logger[IO],
      cs: ContextShift[IO]
  ): Stream[IO, Ref[IO, Map[A, B]]] =
    for {
      cached <- readJsonFileToA[IO, List[B]](path).map(ls => ls.flatMap(b => toTuple(b)).toMap).handleErrorWith { error =>
        Stream.eval(logger.info(s"$path not found")) >> Stream.emit(Map.empty[A, B]).covary[IO]
      }
      ref <- Stream.resource(
        Resource.make(Ref.of[IO, Map[A, B]](cached))(
          ref => flushCache(path, blockingEc, ref).compile.drain
        )
      )
    } yield ref

  def flushCache[A, B: Decoder: Encoder](path: Path, blockingEc: ExecutionContext, ref: Ref[IO, Map[A, B]])(implicit cs: ContextShift[IO]): Stream[IO, Unit] =
    Stream
      .eval(ref.get)
      .flatMap(x => Stream.emits(x.values.toSet.asJson.pretty(Printer.noSpaces).getBytes("UTF-8")))
      .through(fs2.io.file.writeAll[IO](path, blockingEc))

  implicit val eqLocalDirectory: Eq[LocalDirectory] = Eq.instance((p1, p2) => p1.path.toString == p2.path.toString)
  implicit def loggerToContextLogger[F[_]](logger: Logger[F]): ContextLogger[F] = ContextLogger(logger)
}
