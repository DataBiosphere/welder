package org.broadinstitute.dsp.workbench.welder

import cats.effect.std.Semaphore
import cats.effect.{IO, Ref, Resource}
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.LocalBaseDirectory
import org.http4s.blaze.client
import org.http4s.client.middleware.{Logger => Http4sLogger}
import org.typelevel.log4cats.StructuredLogger

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.duration.DurationInt

package object server {
  type Permits = Ref[IO, Map[RelativePath, Semaphore[IO]]]

  private[server] def initStorageAlg(config: AppConfig, blockerBound: Semaphore[IO])(
      implicit logger: StructuredLogger[IO]
  ): Resource[IO, InitStorageAlgResp] = {
    val algConfig = StorageAlgConfig(config.objectService.workingDirectory)

    config match {
      case _: AppConfig.Gcp =>
        GoogleStorageService
          .fromApplicationDefault(Some(blockerBound))
          .map(s => InitStorageAlgResp(CloudStorageAlg.forGoogle(algConfig, s), None))
      case conf: AppConfig.Azure =>
        val azureConfig = AzureStorageConfig(10 minutes, 10 minutes)
        for {
          httpClient <- client
            .BlazeClientBuilder[IO]
            .withRetries(5)
            .resource
          client = Http4sLogger[IO](logHeaders = true, logBody = false)(
            httpClient
          )
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
          storagelink = StorageLink(
            LocalBaseDirectory(RelativePath(Paths.get(""))),
            None,
            CloudStorageDirectory(CloudStorageContainer(workspaceStorageContainer.value), Some(BlobPath("analyses"))),
            ".*\\.ipynb".r //TODO: this should be updated to include .Rmd, .R
          )
        } yield InitStorageAlgResp(CloudStorageAlg.forAzure(algConfig, azureStorageService), Some(storagelink))
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

final case class InitStorageAlgResp(cloudStorageAlg: CloudStorageAlg, storageLink: Option[StorageLink])
