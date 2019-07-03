package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import io.chrisdavenport.linebacker.Linebacker
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.ExecutionContext

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val logger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
      blockingEc <- Stream.resource[IO, ExecutionContext](ExecutionContexts.cachedThreadPool)
      implicit0(it: Linebacker[IO]) <- Stream.eval(Linebacker.bounded(Linebacker.fromExecutionContext[IO](blockingEc), 255))
      appConfig <- Stream.fromEither[IO](Config.appConfig)
      storageLinksCache <- cachedResource[RelativePath, StorageLink](
                       appConfig.pathToStorageLinksJson,
                       blockingEc,
                       storageLink => List(storageLink.localBaseDirectory.path -> storageLink, storageLink.localSafeModeBaseDirectory.path -> storageLink)
      )
      metadataCache <- cachedResource[RelativePath, AdaptedGcsMetadataCache](
                       appConfig.pathToGcsMetadataJson,
                       blockingEc,
                       metadata => List(metadata.localPath -> metadata)
      )
      welderApp <- initWelderApp(appConfig, blockingEc, storageLinksCache, metadataCache)
      serverStream = BlazeServerBuilder[IO].bindHttp(appConfig.serverPort, "0.0.0.0").withHttpApp(welderApp.service).serve
      cleanUpCache = BackgroundTask.cleanUpLock(metadataCache, appConfig.cleanUpLockFrequency)
      _ <- Stream(cleanUpCache, serverStream.drain).covary[IO].parJoin(2)
    } yield ()

    app
      .handleErrorWith { error =>
        Stream.eval(Logger[IO].error(error)("Failed to start server")) >> Stream.raiseError[IO](error)
      }
      .compile
      .drain
      .as(ExitCode.Success)
  }

  def initWelderApp(appConfig: AppConfig, blockingEc: ExecutionContext, storageLinksCache: StorageLinksCache, metadataCache: MetadataCache)(implicit logger: Logger[IO]): Stream[IO, WelderApp] = for {
    implicit0(it: Linebacker[IO]) <- Stream.eval(Linebacker.bounded(Linebacker.fromExecutionContext[IO](blockingEc), 255))
    googleStorageService <- Stream.resource(GoogleStorageService.fromApplicationDefault())
  } yield {
    val storageLinksService = StorageLinksService(storageLinksCache)
    val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(appConfig.objectService.workingDirectory), googleStorageService)
    val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
    val objectService = ObjectService(appConfig.objectService, googleStorageAlg, blockingEc, storageLinkAlg, metadataCache)
    val cacheService = CacheService(
      CachedServiceConfig(appConfig.pathToStorageLinksJson, appConfig.pathToGcsMetadataJson),
      storageLinksCache,
      metadataCache,
      blockingEc
    )
    WelderApp(objectService, storageLinksService, cacheService)
  }
}
