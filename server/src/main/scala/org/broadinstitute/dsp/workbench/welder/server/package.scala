package org.broadinstitute.dsp.workbench.welder

import cats.effect.std.Semaphore
import cats.effect.{IO, Ref, Resource}
import org.broadinstitute.dsde.workbench.azure.{AzureStorageConfig, AzureStorageService, ContainerAuthConfig, EndpointUrl, SasToken}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.http4s.blaze.client
import org.http4s.client.middleware.{Retry, RetryPolicy, Logger => Http4sLogger}
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID
import scala.concurrent.duration.DurationInt

package object server {
  type Permits = Ref[IO, Map[RelativePath, Semaphore[IO]]]

  private[server] def initStorageAlg(config: AppConfig, blockerBound: Semaphore[IO])(implicit logger: StructuredLogger[IO]): Resource[IO, CloudStorageAlg] = {
    val algConfig = StorageAlgConfig(config.objectService.workingDirectory)
    val retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(30 seconds, 5))

    config match {
      case _: AppConfig.Gcp =>
        GoogleStorageService
          .fromApplicationDefault(Some(blockerBound))
          .map(s => CloudStorageAlg.forGoogle(algConfig, s))
      case conf: AppConfig.Azure =>
        val azureConfig = AzureStorageConfig(10 minutes, 10 minutes)
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
          workspaceContainerAuthConfig <- getContainerAuthConfig(
            miscHttpClient,
            petAccessTokenResp.accessToken,
            conf.workspaceStorageContainerResourceId
          )
          workspaceStagingContainerAuthConfig <- getContainerAuthConfig(
            miscHttpClient,
            petAccessTokenResp.accessToken,
            conf.stagingStorageContainerResourceId
          )
//          workspaceContainerAuthConfig = ContainerAuthConfig(
//            SasToken("sp=r&st=2022-08-24T19:19:30Z&se=2022-08-25T03:19:30Z&spr=https&sv=2021-06-08&sr=c&sig=6HZvCu7FnFmCDML1jqLNxoNDVVftZ96LGPI%2FHS6MaBc%3D"),
//            EndpointUrl(
//              "https://sa645b86de374320be204a.blob.core.windows.net/sc-645b86de-7a2b-4c59-aefe-374320be204a?sp=r&st=2022-08-24T19:19:30Z&se=2022-08-25T03:19:30Z&spr=https&sv=2021-06-08&sr=c&sig=6HZvCu7FnFmCDML1jqLNxoNDVVftZ96LGPI%2FHS6MaBc%3D"
//            )
//          )
          workspaceStorageContainer <- Resource.eval(
            IO.fromEither(getStorageContainerNameFromUrl(EndpointUrl(workspaceContainerAuthConfig.endpointUrl.value)))
          )
          azureStorageService <- AzureStorageService.fromSasToken[IO](
            azureConfig,
            Map(
              workspaceStorageContainer -> workspaceContainerAuthConfig,
              config.stagingBucketName.asAzureCloudContainer -> workspaceStagingContainerAuthConfig
            )
          )
        } yield CloudStorageAlg.forAzure(algConfig, azureStorageService)
    }
  }

  private def getContainerAuthConfig(
      miscHttpClient: MiscHttpClientAlg,
      accessToken: PetAccessToken,
      containerResourceId: UUID
  ): Resource[IO, ContainerAuthConfig] =
    for {

      workspaceStorageContainerSasTokenResp <- Resource.eval(
        miscHttpClient.getSasUrl(accessToken, containerResourceId)
      )
      workspaceContainerAuthConfig = ContainerAuthConfig(
        SasToken(workspaceStorageContainerSasTokenResp.token.value),
        EndpointUrl(workspaceStorageContainerSasTokenResp.uri.toString())
      )
    } yield workspaceContainerAuthConfig
}
