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
    config match {
      case _: AppConfig.Gcp =>
        GoogleStorageService
          .fromApplicationDefault(Some(blockerBound))
          .map(s => CloudStorageAlg.forGoogle(algConfig, s))
      case conf: AppConfig.Azure =>
        val azureConfig = AzureStorageConfig(10 minutes, 10 minutes)
        for {
          workspaceContainerAuthConfig <- getContainerAuthConfig(conf.workspaceStorageContainerResourceId, conf.miscHttpClientConfig)
          workspaceStagingContainerAuthConfig <- getContainerAuthConfig(conf.stagingStorageContainerResourceId, conf.miscHttpClientConfig)
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

  private def getContainerAuthConfig(containerResourceId: UUID, conf: MiscHttpClientConfig): Resource[IO, ContainerAuthConfig] = {
    val retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(30 seconds, 5))

    for {
      httpClient <- client
        .BlazeClientBuilder[IO]
        .resource
      httpClientWithLogging = Http4sLogger[IO](logHeaders = true, logBody = false)(
        httpClient
      )
      client = Retry(retryPolicy)(httpClientWithLogging)
      miscHttpClient = new MiscHttpClientInterp(client, conf)
      petAccessTokenResp <- Resource.eval(miscHttpClient.getPetAccessToken())
      workspaceStorageContainerSasTokenResp <- Resource.eval(
        miscHttpClient.getSasUrl(petAccessTokenResp.accessToken, containerResourceId)
      )
      workspaceContainerAuthConfig = ContainerAuthConfig(
        SasToken(workspaceStorageContainerSasTokenResp.token.value),
        EndpointUrl(workspaceStorageContainerSasTokenResp.uri.toString())
      )
    } yield workspaceContainerAuthConfig
  }
}
