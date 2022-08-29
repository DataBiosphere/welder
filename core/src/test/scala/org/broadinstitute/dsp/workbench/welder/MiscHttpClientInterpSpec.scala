package org.broadinstitute.dsp.workbench.welder

import io.circe.parser.decode
import org.broadinstitute.dsde.workbench.azure.SasToken
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.broadinstitute.dsp.workbench.welder.MiscHttpClientAlgCodec._
import org.http4s.Uri

class MiscHttpClientInterpSpec extends AnyFlatSpec with ScalaCheckPropertyChecks with WelderTestSuite {
  it should "decode pet access token properly" in {
    val inputString =
      """
        |{
        |    "access_token": "token",
        |    "client_id": "2c6fa3a7-e528-4fb8-96ea-89608ba0bbe6",
        |    "expires_in": "85100",
        |    "expires_on": "1659465003",
        |    "ext_expires_in": "86399",
        |    "not_before": "1659378303",
        |    "resource": "https://management.azure.com/",
        |    "token_type": "Bearer"
        |}
        |""".stripMargin

    val expected = PetAccessTokenResp(PetAccessToken("token"))
    val res = decode[PetAccessTokenResp](inputString)
    res shouldBe (Right(expected))
  }

  it should "decode SasTokenResp properly" in {
    val inputString =
      """
        |{
        |    "token": "sas_token",
        |    "url": "https://sa445eb6b22667ee38cb8e.blob.core.windows.net/sc-445eb6b2-7452-4d86-a462-2667ee38cb8e?sp=r&st=2022-08-02T20:06:13Z&se=2022-08-03T04:06:13Z&spr=https&sv=2021-06-08&sr=c&sig=asdf"
        |}
        |""".stripMargin
    val expected = SasTokenResp(
      Uri.unsafeFromString(
        "https://sa445eb6b22667ee38cb8e.blob.core.windows.net/sc-445eb6b2-7452-4d86-a462-2667ee38cb8e?sp=r&st=2022-08-02T20:06:13Z&se=2022-08-03T04:06:13Z&spr=https&sv=2021-06-08&sr=c&sig=asdf"
      ),
      SasToken("sas_token")
    )
    val res = decode[SasTokenResp](inputString)
    res shouldBe (Right(expected))
  }
}
