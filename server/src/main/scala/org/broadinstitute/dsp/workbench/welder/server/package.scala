package org.broadinstitute.dsp.workbench.welder

import cats.effect.std.Semaphore
import cats.effect.{IO, Ref, Resource}
import org.broadinstitute.dsde.workbench.azure.{AzureStorageConfig, AzureStorageService, EndpointUrl, SasToken}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.http4s.blaze.client
import org.http4s.client.middleware.{Retry, RetryPolicy, Logger => Http4sLogger}
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.duration.DurationInt

package object server {
  type Permits = Ref[IO, Map[RelativePath, Semaphore[IO]]]

  private[server] def initStorageAlg(config: AppConfig, blockerBound: Semaphore[IO])(implicit logger: StructuredLogger[IO]): Resource[IO, CloudStorageAlg] = {
    val retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(30 seconds, 5))
    val algConfig = StorageAlgConfig(config.objectService.workingDirectory)
    config match {
      case conf: AppConfig.Gcp =>
        GoogleStorageService
          .fromApplicationDefault(Some(blockerBound))
          .map(s => CloudStorageAlg.forGoogle(algConfig, s))
      case conf: AppConfig.Azure =>
        for {
          httpClient <- client
            .BlazeClientBuilder[IO]
            .resource
          httpClientWithLogging = Http4sLogger[IO](logHeaders = true, logBody = false)(
            httpClient
          )
          client = Retry(retryPolicy)(httpClientWithLogging)
          miscHttpClient = new MiscHttpClientInterp(client, conf.miscHttpClientConfig)
          petAccessTokenResp <- Resource.eval(miscHttpClient.getPetAccessToken())
          sasTokenResp <- Resource.eval(miscHttpClient.getSasUrl(petAccessTokenResp.accessToken))
          //TODO: transform sas token resp into tok[en and url
          azureConfig = AzureStorageConfig(10 minutes, SasToken(sasTokenResp.uri.toString()), EndpointUrl(sasTokenResp.uri.toString()))
          azureStorageService <- AzureStorageService.fromSasToken[IO](azureConfig)
        } yield CloudStorageAlg.forAzure(algConfig, azureStorageService)
    }
  }
}
