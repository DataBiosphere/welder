package org.broadinstitute.dsp.workbench.welder

import java.time.Instant

import io.circe.{Decoder, Encoder}
import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.http4s.Uri

final case class LocalObjectPath(asString: String) extends AnyVal
sealed abstract class SyncStatus extends Product with Serializable
object SyncStatus {
  final case object Live {
    override def toString: String = "LIVE"
  }
  final case object Desynchronized {
    override def toString: String = "DESYNCHRONIZED"
  }
  final case object DeletedRemote {
    override def toString: String = "DELETED_REMOTE"
  }

  val stringToSyncStatus = sealerate.values[SyncStatus]
}

object JsonCodec {
  implicit val localObjectPathDecoder: Decoder[LocalObjectPath] = Decoder.decodeString.map(LocalObjectPath)
  implicit val workbenchEmailEncoder: Encoder[WorkbenchEmail] = Encoder.encodeString.contramap(_.value)
  implicit val uriEncoder: Encoder[Uri] = Encoder.encodeString.contramap(_.renderString)
  implicit val instanceEncoder: Encoder[Instant] = Encoder.encodeLong.contramap(_.toEpochMilli) //TODO: shall we make this easier for user to read
  implicit val syncStatusEncoder: Encoder[SyncStatus] = Encoder.encodeString.contramap(_.toString)
}
