package org.broadinstitute.dsp.workbench.welder

import io.circe.Decoder

final case class LocalObjectPath(asString: String) extends AnyVal

object JsonCodec {
  implicit val localObjectPathDecoder: Decoder[LocalObjectPath] = Decoder.decodeString.map(LocalObjectPath)
}