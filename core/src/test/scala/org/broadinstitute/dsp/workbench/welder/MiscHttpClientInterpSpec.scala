package org.broadinstitute.dsp.workbench.welder

import _root_.io.circe.parser.decode
import _root_.io.circe.syntax._
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import fs2._
import org.broadinstitute.dsde.workbench.azure.SasToken
import org.broadinstitute.dsp.workbench.welder.MiscHttpClientAlgCodec._
import org.http4s.client.Client
import org.http4s.headers.`Content-Length`
import org.http4s.{Entity, Headers, Response, Uri}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.nio.charset.Charset
import java.util.{Base64, UUID}
import scala.concurrent.duration._

class MiscHttpClientInterpSpec extends AnyFlatSpec with ScalaCheckPropertyChecks with WelderTestSuite {
  val petId = "pet-1234"
  val petToken = "pet-token"
  val config = MiscHttpClientConfig(Uri.unsafeFromString("https://wsm"), "https://management.azure.com/", UUID.randomUUID(), 1 hour)

  def mockClient(petIdInUserData: Boolean): Client[IO] = Client.apply[IO] { request =>
    val responseBody = request.uri.path.segments.map(_.toString) match {
      case Vector("metadata", "identity", "oauth2", "token") =>
        Map("access_token" -> petToken).asJson.noSpaces.getBytes(Charset.defaultCharset())
      case Vector("metadata", "instance", "compute", "userData") if petIdInUserData =>
        Base64.getEncoder.encode(petId.getBytes(Charset.defaultCharset()))
      case _ => Array.empty[Byte]
    }
    Resource.eval(IO(Response[IO](entity = Entity(Stream.emits(responseBody)), headers = Headers(`Content-Length`.fromLong(responseBody.length.toLong)))))
  }

  it should "get the pet managed identity id from userData" in {
    val client = new MiscHttpClientInterp(mockClient(true), config)
    val returnedPetId = client.getPetManagedIdentityId().unsafeRunSync()
    returnedPetId shouldBe Some(petId)
  }

  it should "not get the pet managed identity id if userData is empty" in {
    val client = new MiscHttpClientInterp(mockClient(false), config)
    val returnedPetId = client.getPetManagedIdentityId().unsafeRunSync()
    returnedPetId shouldBe None
  }

  it should "get a pet access token when userData is present" in {
    val client = new MiscHttpClientInterp(mockClient(true), config)
    val returnedPetToken = client.getPetAccessToken().unsafeRunSync()
    returnedPetToken shouldBe PetAccessTokenResp(PetAccessToken(petToken))
  }

  it should "get a pet access token when userData is not present" in {
    val client = new MiscHttpClientInterp(mockClient(false), config)
    val returnedPetToken = client.getPetAccessToken().unsafeRunSync()
    returnedPetToken shouldBe PetAccessTokenResp(PetAccessToken(petToken))
  }

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
