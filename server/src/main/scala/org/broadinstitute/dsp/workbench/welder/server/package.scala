package org.broadinstitute.dsp.workbench.welder

import cats.effect.std.Semaphore
import cats.effect.{IO, Ref, Resource}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.http4s.blaze.client
import org.http4s.client.middleware.{Retry, RetryPolicy, Logger => Http4sLogger}
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.duration.DurationInt

package object server {
  type Permits = Ref[IO, Map[RelativePath, Semaphore[IO]]]

  private[server] def initStorageAlg(config: AppConfig, blockerBound: Semaphore[IO])(implicit logger: StructuredLogger[IO]): Resource[IO, CloudStorageAlg] = {
    val retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(30 seconds, 5))

    config.cloudProvider match {
      case CloudProvider.Gcp =>
        GoogleStorageService
          .fromApplicationDefault(Some(blockerBound))
          .map(s => CloudStorageAlg.forGoogle(GoogleStorageAlgConfig(config.objectService.workingDirectory), s))
      case CloudProvider.Azure =>
        for {
          httpClient <- client
            .BlazeClientBuilder[IO]
            .resource
          httpClientWithLogging = Http4sLogger[IO](logHeaders = true, logBody = false)(
            httpClient
          )
          client = Retry(retryPolicy)(httpClientWithLogging)
          miscHttpClient = new MiscHttpClientInterp(client, config.miscHttpClientConfig)
          petAccessTokenResp <- Resource.eval(miscHttpClient.getPetAccessToken())
          sasTokenResp <- Resource.eval(miscHttpClient.getSasUrl(petAccessTokenResp.accessToken))
          //TODO: Justin
        } yield ???
    }
  }
}
