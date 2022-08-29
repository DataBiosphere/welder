package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import io.circe.Decoder
import org.http4s.Uri
import java.util.UUID

import cats.implicits._
import org.broadinstitute.dsde.workbench.azure.SasToken

trait MiscHttpClientAlg {
  def getPetAccessToken(): IO[PetAccessTokenResp]
  def getSasUrl(petAccessToken: PetAccessToken): IO[SasTokenResp]
}

final case class MiscHttpClientConfig(wsmUrl: Uri, workspaceId: UUID, storageContainerResourceId: UUID)
final case class PetAccessToken(value: String) extends AnyVal
final case class PetAccessTokenResp(accessToken: PetAccessToken)
final case class SasTokenResp(uri: Uri, token: SasToken)

object MiscHttpClientAlgCodec {
  implicit val decodePetAccessToken: Decoder[PetAccessToken] = Decoder.decodeString.map(PetAccessToken(_))
  implicit val decodeUri: Decoder[Uri] = Decoder.decodeString.emap(s => Uri.fromString(s).leftMap(_.toString))
  implicit val decodePetAccessTokenResp: Decoder[PetAccessTokenResp] = Decoder.forProduct1("access_token")(PetAccessTokenResp.apply)
  implicit val decodeSasToken: Decoder[SasToken] = Decoder.decodeString.map(SasToken)
  implicit val decodeSasTokenResp: Decoder[SasTokenResp] = Decoder.forProduct2("url", "token")(SasTokenResp.apply)
}
