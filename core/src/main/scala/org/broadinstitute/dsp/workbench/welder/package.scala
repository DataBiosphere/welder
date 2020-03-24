package org.broadinstitute.dsp.workbench

import java.io.File
import java.math.BigInteger
import java.nio.file.{Path, Paths, StandardOpenOption}
import java.security.MessageDigest
import java.util.Base64

import cats.Eq
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, ContextShift, IO, Resource, Sync}
import cats.implicits._
import fs2.{Pipe, Stream}
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Printer}
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath

package object welder {
  val LAST_LOCKED_BY = "lastLockedBy"
  val LOCK_EXPIRES_AT = "lockExpiresAt"

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

  /**
    * @param localBaseDirectory local base directory
    * @param blobPath blob path defined in CloudStorageDirectory
    * @param blobName actual blob name
    * @return
    */
  def getLocalPath(localBaseDirectory: RelativePath, blobPath: Option[BlobPath], blobName: String): Either[Throwable, RelativePath] =
    for {
      localName <- blobPath match {
        case Some(bp) => Either.catchNonFatal(Paths.get(bp.asString).relativize(Paths.get(blobName)))
        case None => Right(Paths.get(blobName))
      }
      localPath <- Either.catchNonFatal(localBaseDirectory.asPath.resolve(localName))
    } yield RelativePath(localPath)

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
    HashedLockedBy(String.format("%064x", new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(metadata.getBytes("UTF-8")))))
  }

  type StorageLinksCache = Ref[IO, Map[RelativePath, StorageLink]]
  type MetadataCache = Ref[IO, Map[RelativePath, AdaptedGcsMetadataCache]]

  val gcpObjectType = "text/plain"

  private[welder] def mkdirIfNotExist(path: java.nio.file.Path): IO[Unit] = {
    val directory = new File(path.toString)
    if (!directory.exists) {
      IO(directory.mkdirs).void
    } else IO.unit
  }

  private[welder] def cachedResource[A, B: Decoder: Encoder](
      googleStorageAlg: GoogleStorageAlg,
      stagingBucketName: GcsBucketName,
      blobName: GcsBlobName,
      blocker: Blocker,
      toTuple: B => List[(A, B)]
  )(
      implicit cs: ContextShift[IO]
  ): Stream[IO, Ref[IO, Map[A, B]]] =
    for {
      cached <- googleStorageAlg.getBlob[List[B]](stagingBucketName, blobName).map(ls => ls.flatMap(b => toTuple(b)).toMap)
      ref <- Stream.resource(
        Resource.make(Ref.of[IO, Map[A, B]](cached))(
          ref => flushCache(googleStorageAlg, stagingBucketName, blobName, blocker, ref).compile.drain
        )
      )
    } yield ref

  def flushCache[A, B: Decoder: Encoder](
      googleStorageAlg: GoogleStorageAlg,
      stagingBucketName: GcsBucketName,
      blobName: GcsBlobName,
      blocker: Blocker,
      ref: Ref[IO, Map[A, B]]
  ): Stream[IO, Unit] =
    for {
      data <- Stream.eval(ref.get)
      _ <- googleStorageAlg.uploadBlob(stagingBucketName, blobName, data.values.toSet.asJson.printWith(Printer.noSpaces).getBytes("UTF-8"))
    } yield ()

  /**
    * Example:
    * scala> findFilesWithSuffix(res1, ".log")
    * res5: List[java.io.File] = List(/tmp/d.log, /tmp/f.log)
    */
  def findFilesWithSuffix(parent: Path, suffix: String): List[File] =
    parent.toFile.listFiles().filter(f => f.isFile && f.getName.endsWith(suffix)).toList

  private[welder] val writeFileOptions = List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

  implicit val eqLocalDirectory: Eq[LocalDirectory] = Eq.instance((p1, p2) => p1.path.toString == p2.path.toString)
}
