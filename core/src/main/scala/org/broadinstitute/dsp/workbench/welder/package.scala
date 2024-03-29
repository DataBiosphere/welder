package org.broadinstitute.dsp.workbench

import cats.Eq
import cats.effect.{IO, Ref, Resource, Sync}
import cats.implicits._
import cats.mtl.Ask
import fs2.io.file.Flags
import fs2.{Pipe, Stream}
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Printer}
import org.broadinstitute.dsde.workbench.azure.{ContainerName, EndpointUrl}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2
import org.typelevel.log4cats.StructuredLogger

import java.io.File
import java.math.BigInteger
import java.nio.file.{Path, Paths}
import java.security.MessageDigest
import java.util.Base64
import scala.util.matching.Regex

package object welder {
  val LAST_LOCKED_BY = "lastLockedBy"
  val LOCK_EXPIRES_AT = "lockExpiresAt"
  val TRACE_ID_LOGGING_KEY = "traceId"

  //Input is in the format of "<bucketName>" or "<bucketName>/<blobName>"
  def parseCloudStorageContainer(str: String): Either[String, CloudStorageContainer] =
    for {
      splitted <- Either.catchNonFatal(str.split("/")).leftMap(_.getMessage)
      bucketName <- Either
        .catchNonFatal(splitted(0))
        .leftMap(_ => s"failed to parse bucket name")
        .ensure("bucketName can't be empty")(s => s.nonEmpty)
    } yield CloudStorageContainer(bucketName)

  def parseCloudBlobPath(str: String): Either[String, CloudBlobPath] = {
    val parsed = str.replaceAll("gs://", "")

    for {
      bucketName <- parseCloudStorageContainer(parsed)
      length = s"${bucketName.name}/".length
      objectName <- Either
        .catchNonFatal(parsed.drop(length))
        .leftMap(_ => s"failed to parse object name.")
        .ensure("objectName can't be empty")(s => s.nonEmpty)
    } yield CloudBlobPath(bucketName, CloudStorageBlob(objectName))
  }

  // When the local path is in the format of "<workspaceName>/<fileName>", this should return List("workspaceName");
  // When the local path is in the format of "<fileName>", this should return List("")
  def getPossibleBaseDirectory(localPath: Path): List[Path] = {
    val res = ((localPath.getNameCount - 1)
      .to(1, -1))
      .map(index => localPath.subpath(0, index))
      .toList

    if (res.size == 0)
      List(Paths.get(""))
    else res
  }

  def getFullBlobName(basePath: RelativePath, localPath: Path, blobPath: Option[BlobPath]): CloudStorageBlob = {
    val subPath = basePath.asPath.relativize(localPath)
    blobPath match {
      case Some(bp) => CloudStorageBlob(bp.asString + "/" + subPath.toString)
      case None => CloudStorageBlob(subPath.toString)
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

  def getCloudBlobPath(localObjectPath: RelativePath, basePathAndStorageLink: CommonContext): CloudBlobPath = {
    val fullBlobName =
      getFullBlobName(basePathAndStorageLink.basePath, localObjectPath.asPath, basePathAndStorageLink.storageLink.cloudStorageDirectory.blobPath)
    CloudBlobPath(basePathAndStorageLink.storageLink.cloudStorageDirectory.container, fullBlobName)
  }

  //Note that bucketName below does NOT include the gs:// prefix
  //This string will be hashed when stored in GCS metadata
  def lockedByString(bucketName: CloudStorageContainer, ownerEmail: WorkbenchEmail): String = bucketName.name + ":" + ownerEmail.value

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
      cloudStorageAlgRef: Ref[IO, CloudStorageAlg],
      cloudBlobPath: CloudBlobPath,
      toTuple: B => List[(A, B)]
  )(
      implicit logger: StructuredLogger[IO],
      ev: Ask[IO, TraceId]
  ): Stream[IO, Ref[IO, Map[A, B]]] =
    for {
      ctx <- Stream.eval(ev.ask)
      storageAlg <- Stream.eval(cloudStorageAlgRef.get)
      // We're previously reading and persisting cache from/to local disk, but this can be problematic when disk space runs out.
      // Hence we're persisting cache to GCS. Since leonardo tries to automatically upgrade welder version, we'll need to support both cases.
      // We first try to read cache from local disk, if it exists, use it; if not, we read cache from gcs
      // Code for reading from disk can be deleted once we're positive that all user clusters are upgraded or when we no longer care.
      // This change is made on 3/26/2020
      traceId <- Stream.eval(ev.ask)
      _ <- Stream.eval(logger.info(Map("traceId" -> ctx.asString))(s"Downloading cache from ${cloudBlobPath}"))
      cacheFromDisk <- localCache[B](Paths.get(s"/work/.welder/${cloudBlobPath.blobPath.name.split("/")(1)}"))

      loadedCache <- cacheFromDisk.fold {
        storageAlg
          .getBlob[List[B]](cloudBlobPath)
          .last
          .map(_.getOrElse(List.empty)) // The first time welder starts up, there won't be any existing cache, hence returning empty list
      }(x => Stream.eval(IO.pure(x)))

      cached = loadedCache.flatMap(b => toTuple(b)).toMap
      ref <- Stream.resource(
        Resource.make(Ref.of[IO, Map[A, B]](cached))(ref => flushCache(storageAlg, cloudBlobPath, ref))
      )
    } yield ref

  private def localCache[B: Decoder](path: Path)(
      implicit logger: StructuredLogger[IO]
  ): Stream[IO, Option[List[B]]] =
    for {
      res <- if (path.toFile.exists()) {
        for {
          cached <- util2.readJsonFileToA[IO, List[B]](path).handleErrorWith { error =>
            error match {
              case e => Stream.eval(logger.info(e)(s"Error reading $path")) >> Stream.raiseError[IO](e)
            }
          }
        } yield Some(cached)
      } else Stream.eval(IO.pure(none[List[B]]))
    } yield res

  def flushCache[A, B: Decoder: Encoder](
      googleStorageAlg: CloudStorageAlg,
      cloudBlobPath: CloudBlobPath,
      ref: Ref[IO, Map[A, B]]
  )(implicit logger: StructuredLogger[IO], ev: Ask[IO, TraceId]): IO[Unit] =
    for {
      _ <- logger.info(s"flushing cache to $cloudBlobPath")
      data <- ref.get
      bytes = Stream.emits(data.values.toSet.asJson.printWith(Printer.noSpaces).getBytes("UTF-8")).covary[IO]
      _ <- (bytes through googleStorageAlg.uploadBlob(cloudBlobPath)).compile.drain
      // We're previously reading and persisting cache from/to local disk, but this can be problematic when disk space runs out.
      // Hence we're persisting cache to GCS. Since leonardo tries to automatically upgrade welder version, we'll need to support both cases.
      // We first try to read cache from local disk, if it exists, use it; if not, we read cache from gcs
      // Code for reading from disk can be deleted once we're positive that all user clusters are upgraded or when we no longer care.
      // This change is made on 3/26/2020
      legacyCacheDir = new File(s"/work/.welder")
      files = legacyCacheDir.listFiles()
      _ <- if (files != null)
        IO(files.toList.foreach(_.delete()))
      else IO.unit
    } yield ()

  /**
    * Example:
    * scala> findFilesWithSuffix(res1, ".log")
    * res5: List[java.io.File] = List(/tmp/d.log, /tmp/f.log)
    */
  def findFilesWithSuffix(parent: Path, suffix: String): List[File] =
    parent.toFile.listFiles().filter(f => f.isFile && f.getName.endsWith(suffix)).toList

  def findFilesWithPattern(parent: Path, pattern: Regex): List[File] =
    parent.toFile.listFiles().filter(f => f.isFile && pattern.findFirstIn(f.getName).isDefined).toList

  def getStorageContainerNameFromUrl(url: EndpointUrl): Either[Throwable, ContainerName] =
    for {
      splittedString <- Either.catchNonFatal(url.value.stripPrefix("https://").split("/")(1))
      res <- Either.catchNonFatal(splittedString.split("\\?")(0))
    } yield ContainerName(res)

  private[welder] val writeFileOptions = Flags.Write

  implicit val eqLocalDirectory: Eq[LocalDirectory] = Eq.instance((p1, p2) => p1.path.toString == p2.path.toString)
}
