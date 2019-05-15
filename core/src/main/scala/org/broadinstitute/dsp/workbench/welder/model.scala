package org.broadinstitute.dsp.workbench.welder

import java.time.Instant

import cats.implicits._
import io.circe.{Decoder, Encoder}
import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.http4s.Uri

final case class LocalObjectPath(asString: String) extends AnyVal
sealed abstract class SyncStatus extends Product with Serializable
object SyncStatus {
  final case object Live extends SyncStatus {
    override def toString: String = "LIVE"
  }
  final case object Desynchronized extends SyncStatus {
    override def toString: String = "DESYNCHRONIZED"
  }
  final case object DeletedRemote extends SyncStatus {
    override def toString: String = "DELETED_REMOTE"
  }

  val stringToSyncStatus: Set[SyncStatus] = sealerate.values[SyncStatus]
}
final case class BucketNameAndObjectName(bucketName: GcsBucketName, blobName: GcsBlobName)

object JsonCodec {
  implicit val localObjectPathDecoder: Decoder[LocalObjectPath] = Decoder.decodeString.map(LocalObjectPath)
  implicit val workbenchEmailEncoder: Encoder[WorkbenchEmail] = Encoder.encodeString.contramap(_.value)
  implicit val uriEncoder: Encoder[Uri] = Encoder.encodeString.contramap(_.renderString)
  implicit val uriDecoder: Decoder[Uri] = Decoder.decodeString.emap(s => Uri.fromString(s).leftMap(_.getMessage()))
  implicit val instanceEncoder: Encoder[Instant] = Encoder.encodeLong.contramap(_.toEpochMilli) //TODO: shall we make this easier for user to read
  implicit val syncStatusEncoder: Encoder[SyncStatus] = Encoder.encodeString.contramap(_.toString)
  implicit val gcsBucketNameEncoder: Decoder[GcsBucketName] = Decoder.decodeString.map(GcsBucketName)
  implicit val gcsBlobNameEncoder: Decoder[GcsBlobName] = Decoder.decodeString.map(GcsBlobName)
  implicit val bucketNameAndObjectName: Decoder[BucketNameAndObjectName] = Decoder.decodeString.emap{
    str =>
      for {
        parsed <- Either.catchNonFatal(str.split("/")).leftMap(_.getMessage)
        bucketName <- Either.catchNonFatal(parsed(2))
                        .leftMap(_.getMessage)
                        .ensure("bucketName can't be empty")(s => s.nonEmpty)
        objectName <- Either.catchNonFatal(parsed.drop(3).mkString("/"))
                        .leftMap(_.getMessage)
                        .ensure("objectName can't be empty")(s => s.nonEmpty)
      } yield BucketNameAndObjectName(GcsBucketName(bucketName), GcsBlobName(objectName))
  }
}
