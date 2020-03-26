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
import io.chrisdavenport.log4cats.Logger
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Printer}
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.util2
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath

import scala.reflect.io.Directory

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
      implicit cs: ContextShift[IO],
      logger: Logger[IO]
  ): Stream[IO, Ref[IO, Map[A, B]]] = {
    for {
    // We're previously reading and persisting cache from/to local disk, but this can be problematic when disk space runs out.
    // Hence we're persisting cache to GCS. Since leonardo tries to automatically upgrade welder version, we'll need to support both cases.
    // We first try to read cache from local disk, if it exists, use it; if not, we read cache from gcs
    // Code for reading from disk can be deleted once we're positive that all user clusters are upgraded or when we no longer care.
    // This change is made on 3/26/2020
      cacheFromDisk <- localCache[B](Paths.get(s"/work/.welder/${blobName.value.split("/")(1)}"), blocker)
      loadedCache <- cacheFromDisk.fold{
        googleStorageAlg.getBlob[List[B]](stagingBucketName, blobName).last.map(_.getOrElse(List.empty)) // The first time welder starts up, there won't be any existing cache, hence returning empty list
      }(x => Stream.eval(IO.pure(x)))

      cached = loadedCache.flatMap(b => toTuple(b)).toMap
      ref <- Stream.resource(
        Resource.make(Ref.of[IO, Map[A, B]](cached))(
          ref => flushCache(googleStorageAlg, stagingBucketName, blobName, blocker, ref).compile.drain
        )
      )
    } yield ref
  }

  private def localCache[B: Decoder](path: Path, blocker: Blocker)(
    implicit logger: Logger[IO],
    cs: ContextShift[IO]
  ): Stream[IO, Option[List[B]]] =
    for {
      res <- if(path.toFile.exists()){
          for {
            cached <- util2.readJsonFileToA[IO, List[B]](path, Some(blocker)).handleErrorWith { error =>
              error match {
                case e => Stream.eval(logger.info(e)(s"Error reading $path")) >> Stream.raiseError[IO](e)
              }
            }
          } yield Some(cached)
      } else Stream.eval(IO.pure(none[List[B]]))
    } yield res


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
      // We're previously reading and persisting cache from/to local disk, but this can be problematic when disk space runs out.
      // Hence we're persisting cache to GCS. Since leonardo tries to automatically upgrade welder version, we'll need to support both cases.
      // We first try to read cache from local disk, if it exists, use it; if not, we read cache from gcs
      // Code for reading from disk can be deleted once we're positive that all user clusters are upgraded or when we no longer care.
      // This change is made on 3/26/2020
      legacyCacheDir = new Directory(new File(s"/work/.welder"))
      _ <- Stream.eval(IO(legacyCacheDir.deleteRecursively()))
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
