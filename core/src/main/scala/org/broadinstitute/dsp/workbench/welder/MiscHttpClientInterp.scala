package org.broadinstitute.dsp.workbench.welder
import cats.effect.IO
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.streamUntilDoneOrTimeout
import org.http4s.{AuthScheme, Credentials, Header, Headers, Method, Request, Uri}
import org.http4s.client.Client
import org.typelevel.ci.CIString
import org.http4s.QueryParamEncoder.stringQueryParamEncoder
import org.broadinstitute.dsp.workbench.welder.MiscHttpClientAlgCodec.decodePetAccessTokenResp
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.headers.Authorization
import org.broadinstitute.dsp.workbench.welder.MiscHttpClientAlgCodec.decodeSasTokenResp

import java.util.UUID
import scala.concurrent.duration.DurationInt

class MiscHttpClientInterp(httpClient: Client[IO], config: MiscHttpClientConfig) extends MiscHttpClientAlg {
  implicit val doneCheckable: DoneCheckable[Option[PetAccessTokenResp]] = (a: Option[PetAccessTokenResp]) => a.isDefined

  override def getPetAccessToken(): IO[PetAccessTokenResp] = {
    val uri = (Uri.unsafeFromString("http://169.254.169.254") / "metadata" / "identity" / "oauth2" / "token")
      .withQueryParams(
        Map
          .newBuilder[String, String]
          .addAll(
            List("api-version" -> "2018-02-01", "resource" -> "https://management.azure.com/")
          )
          .result()
      )

    val getPetAccessToken = httpClient.expectOption[PetAccessTokenResp](
      Request[IO](
        method = Method.GET,
        uri = uri,
        headers = Headers(Header.Raw.apply(CIString("Metadata"), "true"))
      )
    )
    streamUntilDoneOrTimeout(getPetAccessToken, 10, 10 seconds, "fail to get PET access token").map(_.get)
  }

  override def getSasUrl(petAccessToken: PetAccessToken, storageContainerResourceId: UUID): IO[SasTokenResp] = {
    val uri =
      (config.wsmUrl / "api" / "workspaces" / "v1" / config.workspaceId.toString / "resources" / "controlled" / "azure" / "storageContainer" / storageContainerResourceId.toString / "getSasToken")
    httpClient.expect[SasTokenResp](
      Request[IO](
        method = Method.POST,
        uri = uri,
        headers = Headers(Authorization((Credentials.Token(AuthScheme.Bearer, petAccessToken.value))))
      )
    )
  }
}
