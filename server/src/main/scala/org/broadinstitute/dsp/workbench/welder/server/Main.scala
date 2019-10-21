package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.util2.ExecutionContexts
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.ExecutionContext

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val logger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
      blockingEc <- Stream.resource[IO, ExecutionContext](ExecutionContexts.cachedThreadPool)
      blocker = Blocker.liftExecutionContext(blockingEc)
      appConfig <- Stream.fromEither[IO](Config.appConfig)
      storageLinksCache <- cachedResource[RelativePath, StorageLink](
        appConfig.pathToStorageLinksJson,
        blocker,
        storageLink => List(storageLink.localBaseDirectory.path -> storageLink, storageLink.localSafeModeBaseDirectory.path -> storageLink)
      )
      metadataCache <- cachedResource[RelativePath, AdaptedGcsMetadataCache](
        appConfig.pathToGcsMetadataJson,
        blocker,
        metadata => List(metadata.localPath -> metadata)
      )
      streams <- initStreams(appConfig, blocker, storageLinksCache, metadataCache)
      _ <- Stream.emits(streams).covary[IO].parJoin(streams.length)
    } yield ()

    app
      .handleErrorWith { error =>
        Stream.eval(Logger[IO].error(error)("Failed to start server")) >> Stream.raiseError[IO](error)
      }
      .compile
      .drain
      .as(ExitCode.Error)
  }

  def initStreams(appConfig: AppConfig, blocker: Blocker, storageLinksCache: StorageLinksCache, metadataCache: MetadataCache)(
      implicit logger: Logger[IO]
  ): Stream[IO, List[Stream[IO, Unit]]] =
    for {
      permits <- Stream.eval(Ref[IO].of(Map.empty[RelativePath, Semaphore[IO]]))
      blockerBound <- Stream.eval(Semaphore[IO](255))
      googleStorageService <- Stream.resource(GoogleStorageService.fromApplicationDefault(blocker, Some(blockerBound)))
      shutDownSignal <- Stream.eval(SignallingRef[IO, Boolean](false))
    } yield {
      val metadataCacheAlg = new MetadataCacheInterp(metadataCache)
      val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(appConfig.objectService.workingDirectory), blocker, googleStorageService)
      val storageLinksServiceConfig = StorageLinksServiceConfig(appConfig.objectService.workingDirectory, appConfig.workspaceBucketNameFileName)
      val storageLinksService = StorageLinksService(storageLinksCache, googleStorageAlg, metadataCacheAlg, storageLinksServiceConfig, blocker)
      val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
      val objectService = ObjectService(permits, appConfig.objectService, googleStorageAlg, blocker, storageLinkAlg, metadataCacheAlg)
      val shutdownService = ShutdownService(
        PreshutdownServiceConfig(appConfig.pathToStorageLinksJson, appConfig.pathToGcsMetadataJson, appConfig.objectService.workingDirectory, appConfig.stagingBucketName),
        shutDownSignal,
        storageLinksCache,
        metadataCache,
        googleStorageAlg,
        blocker
      )

      val welderApp = WelderApp(objectService, storageLinksService, shutdownService)
      val serverStream = BlazeServerBuilder[IO]
        .bindHttp(appConfig.serverPort, "0.0.0.0")
        .withHttpApp(welderApp.service)
        .serveWhile(shutDownSignal, Ref.unsafe(ExitCode.Success))

      val backGroundTaskConfig = BackgroundTaskConfig(
        appConfig.objectService.workingDirectory,
        appConfig.cleanUpLockInterval,
        appConfig.flushCacheInterval,
        appConfig.syncCloudStorageDirectoryInterval
      )
      val backGroundTask = new BackgroundTask(backGroundTaskConfig, metadataCache, storageLinksCache, googleStorageAlg, metadataCacheAlg)
      val flushCache = backGroundTask.flushBothCache(
        appConfig.pathToStorageLinksJson,
        appConfig.pathToGcsMetadataJson,
        blocker
      )
      List(backGroundTask.cleanUpLock, flushCache, backGroundTask.syncCloudStorageDirectory, serverStream.drain)
    }
}
