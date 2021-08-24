package org.broadinstitute.dsp.workbench.welder
package server

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.{Logger, StructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.util2.ExecutionContexts
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.ExecutionContext

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val logger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
      blockingEc <- Stream.resource[IO, ExecutionContext](ExecutionContexts.cachedThreadPool[IO])
      blocker = Blocker.liftExecutionContext(blockingEc)
      appConfig <- Stream.fromEither[IO](Config.appConfig)
      streams <- initStreams(appConfig, blocker)
      _ <- Stream.emits(streams).covary[IO].parJoin(streams.length)
    } yield ()

    app
      .handleErrorWith(error => Stream.eval(Logger[IO].error(error)("Failed to start server")) >> Stream.raiseError[IO](error))
      .compile
      .drain
      .as(ExitCode.Error)
  }

  def initStreams(appConfig: AppConfig, blocker: Blocker)(
      implicit logger: StructuredLogger[IO]
  ): Stream[IO, List[Stream[IO, Unit]]] =
    for {
      permits <- Stream.eval(Ref[IO].of(Map.empty[RelativePath, Semaphore[IO]]))
      blockerBound <- Stream.eval(Semaphore[IO](255))
      googleStorageService <- Stream.resource(GoogleStorageService.fromApplicationDefault(blocker, Some(blockerBound)))
      googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(appConfig.objectService.workingDirectory), blocker, googleStorageService)
      storageLinksCache <- cachedResource[RelativePath, StorageLink](
        googleStorageAlg,
        appConfig.stagingBucketName,
        appConfig.storageLinksJsonBlobName,
        blocker,
        storageLink => {
          val safeModeDirectory =
            storageLink.localSafeModeBaseDirectory.fold[List[Tuple2[RelativePath, StorageLink]]](List.empty)(l => List(l.path -> storageLink))
          List(storageLink.localBaseDirectory.path -> storageLink) ++ safeModeDirectory
        }
      )
      metadataCache <- cachedResource[RelativePath, AdaptedGcsMetadataCache](
        googleStorageAlg,
        appConfig.stagingBucketName,
        appConfig.gcsMetadataJsonBlobName,
        blocker,
        metadata => List(metadata.localPath -> metadata)
      )
      shutDownSignal <- Stream.eval(SignallingRef[IO, Boolean](false))
    } yield {
      val metadataCacheAlg = new MetadataCacheInterp(metadataCache)
      val storageLinksServiceConfig = StorageLinksServiceConfig(appConfig.objectService.workingDirectory, appConfig.workspaceBucketNameFileName)
      val storageLinksService = StorageLinksService(storageLinksCache, googleStorageAlg, metadataCacheAlg, storageLinksServiceConfig, blocker)
      val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
      val objectService = ObjectService(permits, appConfig.objectService, googleStorageAlg, blocker, storageLinkAlg, metadataCacheAlg)
      val shutdownService = ShutdownService(
        PreshutdownServiceConfig(
          appConfig.storageLinksJsonBlobName,
          appConfig.gcsMetadataJsonBlobName,
          appConfig.objectService.workingDirectory,
          appConfig.stagingBucketName
        ),
        shutDownSignal,
        storageLinksCache,
        metadataCache,
        googleStorageAlg,
        blocker
      )

      val welderApp = WelderApp(objectService, storageLinksService, shutdownService)
      val serverStream = BlazeServerBuilder[IO](ExecutionContext.global)
        .bindHttp(appConfig.serverPort, "0.0.0.0")
        .withHttpApp(welderApp.service)
        .serveWhile(shutDownSignal, Ref.unsafe[IO, ExitCode](ExitCode.Success))

      val backGroundTaskConfig = BackgroundTaskConfig(
        appConfig.objectService.workingDirectory,
        appConfig.stagingBucketName,
        appConfig.cleanUpLockInterval,
        appConfig.flushCacheInterval,
        appConfig.syncCloudStorageDirectoryInterval,
        appConfig.delocalizeDirectoryInterval,
        appConfig.rstudioRuntime
      )
      val backGroundTask = new BackgroundTask(backGroundTaskConfig, metadataCache, storageLinksCache, googleStorageAlg, metadataCacheAlg)
      val flushCache = backGroundTask.flushBothCache(
        appConfig.storageLinksJsonBlobName,
        appConfig.gcsMetadataJsonBlobName,
        blocker
      )
      List(backGroundTask.cleanUpLock, flushCache, backGroundTask.syncCloudStorageDirectory, backGroundTask.delocalizeBackgroundProcess, serverStream.drain)
    }
}
