package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout}
import org.broadinstitute.dsp.workbench.welder.MiscHttpClientAlgCodec.{decodePetAccessTokenResp, decodeSasTokenResp}
import org.http4s.QueryParamEncoder.stringQueryParamEncoder
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s._
import org.typelevel.ci.CIString

import java.nio.charset.StandardCharsets
import java.util.{Base64, UUID}
import scala.concurrent.duration.DurationInt

class MiscHttpClientInterp(httpClient: Client[IO], config: MiscHttpClientConfig) extends MiscHttpClientAlg {
  implicit def doneCheckable[A]: DoneCheckable[Option[A]] = (a: Option[A]) => a.isDefined

  override def getPetAccessToken(): IO[PetAccessTokenResp] =
    getPetManagedIdentityId().flatMap { petManagedIdentityIdOpt =>
      val queryParams = Map("api-version" -> "2018-02-01", "resource" -> "https://management.azure.com/") ++
        petManagedIdentityIdOpt.map(mi => Map("msi_res_id" -> mi)).getOrElse(Map.empty)
      val uri = (Uri.unsafeFromString("http://169.254.169.254") / "metadata" / "identity" / "oauth2" / "token").withQueryParams(queryParams)

      val getPetAccessToken = httpClient.expectOption[PetAccessTokenResp](
        Request[IO](
          method = Method.GET,
          uri = uri.withQueryParams(queryParams),
          headers = Headers(Header.Raw.apply(CIString("Metadata"), "true"))
        )
      )

      streamUntilDoneOrTimeout(getPetAccessToken, 10, 10 seconds, "fail to get PET access token").map(_.get)
    }

  override def getSasUrl(petAccessToken: PetAccessToken, storageContainerResourceId: UUID): IO[SasTokenResp] = {
    val uri =
      (config.wsmUrl / "api" / "workspaces" / "v1" / config.workspaceId.toString / "resources" / "controlled" / "azure" / "storageContainer" / storageContainerResourceId.toString / "getSasToken")
        .withQueryParam("sasExpirationDuration", config.sasTokenExpiresIn.toSeconds)
    httpClient.expect[SasTokenResp](
      Request[IO](
        method = Method.POST,
        uri = uri,
        headers = Headers(Authorization((Credentials.Token(AuthScheme.Bearer, petAccessToken.value))))
      )
    )
  }

  private def getPetManagedIdentityId(): IO[Option[String]] = {
    val uri = (Uri.unsafeFromString("http://169.254.169.254") / "metadata" / "instance" / "compute" / "userData")
      .withQueryParams(
        Map("api-version" -> "2021-01-01", "format" -> "text")
      )

    val getId = httpClient.expectOption[String](
      Request[IO](
        method = Method.GET,
        uri = uri,
        headers = Headers(Header.Raw.apply(CIString("Metadata"), "true"))
      )
    )
    streamFUntilDone(getId, 5, 2 seconds).compile.lastOrError
      .map(_.map(s => new String(Base64.getDecoder.decode(s), StandardCharsets.UTF_8)))
  }
}
