package org.broadinstitute.dsp.workbench.welder
package server

import java.util.UUID
import cats.effect.kernel.Ref
import cats.effect.std.{Dispatcher, Semaphore}
import cats.effect.unsafe.implicits.global
import cats.effect.{ExitCode, IO, IOApp}
import cats.mtl.Ask
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.workbench.welder.JsonCodec._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, StructuredLogger}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    implicit val logger = Slf4jLogger.getLogger[IO]

    val app: Stream[IO, Unit] = for {
      appConfig <- Stream.fromEither[IO](Config.appConfig)
      streams <- initStreams(appConfig)
      _ <- Stream.emits(streams).covary[IO].parJoin(streams.length)
    } yield ()

    app
      .handleErrorWith(error => Stream.eval(Logger[IO].error(error)("Failed to start server")) >> Stream.raiseError[IO](error))
      .compile
      .drain
      .as(ExitCode.Error)
  }

  def initStreams(appConfig: AppConfig)(implicit
      logger: StructuredLogger[IO]
  ): Stream[IO, List[Stream[IO, Unit]]] = {

    implicit val traceIdImplicit = Ask.const[IO, TraceId](TraceId(UUID.randomUUID().toString))
    for {
      permits <- Stream.eval(Ref[IO].of(Map.empty[RelativePath, Semaphore[IO]]))
      blockerBound <- Stream.eval(Semaphore[IO](255))
      initStorageAlgResp <- Stream.resource(initStorageAlg(appConfig, blockerBound))
      storageAlgRef <- Stream.eval(Ref[IO].of(initStorageAlgResp.cloudStorageAlg))
      storageLinksCache <- appConfig match {
        case _: AppConfig.Azure =>
//        For Azure, all ipynb files will exist under home directory for jupyter. Hence we don't need to dynamically define storagelinks
          Stream.eval(
            Ref.of[IO, Map[RelativePath, StorageLink]](Map(initStorageAlgResp.storageLink.get.localBaseDirectory.path -> initStorageAlgResp.storageLink.get))
          )
        case conf: AppConfig.Gcp =>
          cachedResource[RelativePath, StorageLink](
            storageAlgRef,
            conf.getStorageLinksJsonUri,
            storageLink => {
              val safeModeDirectory =
                storageLink.localSafeModeBaseDirectory.fold[List[Tuple2[RelativePath, StorageLink]]](List.empty)(l => List(l.path -> storageLink))
              List(storageLink.localBaseDirectory.path -> storageLink) ++ safeModeDirectory
            }
          )
      }

      metadataCache <- cachedResource[RelativePath, AdaptedGcsMetadataCache](
        storageAlgRef,
        appConfig.getMetadataJsonBlobNameUri,
        metadata => List(metadata.localPath -> metadata)
      )
      shutDownSignal <- Stream.eval(SignallingRef[IO, Boolean](false))
      dispatcher <- Stream.resource(Dispatcher[IO])
      metadataCacheAlg = new MetadataCacheInterp(metadataCache)

      backGroundTaskConfig = BackgroundTaskConfig(
        appConfig.objectService.workingDirectory,
        appConfig.stagingBucketName,
        appConfig.cleanUpLockInterval,
        appConfig.flushCacheInterval,
        appConfig.syncCloudStorageDirectoryInterval,
        appConfig.delocalizeDirectoryInterval,
        appConfig.isRstudioRuntime,
        appConfig.objectService.ownerEmail
      )
      backGroundTask = new BackgroundTask(backGroundTaskConfig, metadataCache, storageLinksCache, storageAlgRef, metadataCacheAlg)
      _ <- Stream.eval(
        IO(sys.addShutdownHook(backGroundTask.flushBothCacheOnce(appConfig.storageLinksJsonBlobName, appConfig.metadataJsonBlobName).unsafeRunSync()))
      )
    } yield {
      val storageLinksServiceConfig = StorageLinksServiceConfig(appConfig.objectService.workingDirectory, appConfig.workspaceBucketNameFileName)
      val storageLinksService = StorageLinksService(storageLinksCache, storageAlgRef, metadataCacheAlg, storageLinksServiceConfig, dispatcher)
      val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
      val objectService = ObjectService(permits, appConfig.objectService, storageAlgRef, storageLinkAlg, metadataCacheAlg)
      val shutdownService = ShutdownService(
        PreshutdownServiceConfig(
          appConfig.storageLinksJsonBlobName,
          appConfig.metadataJsonBlobName,
          appConfig.objectService.workingDirectory,
          appConfig.stagingBucketName
        ),
        shutDownSignal,
        storageLinksCache,
        metadataCache,
        storageAlgRef
      )

      val welderApp = WelderApp(objectService, storageLinksService, shutdownService)
      val serverStream = org.http4s.blaze.server
        .BlazeServerBuilder[IO]
        .bindHttp(appConfig.serverPort, "0.0.0.0")
        .withHttpApp(welderApp.service)
        .serveWhile(shutDownSignal, Ref.unsafe[IO, ExitCode](ExitCode.Success))
        .drain

      val flushCache = backGroundTask.flushBothCache(
        appConfig.storageLinksJsonBlobName,
        appConfig.metadataJsonBlobName
      )

      appConfig match {
        case _: AppConfig.Gcp =>
          List(backGroundTask.cleanUpLock, flushCache, backGroundTask.syncCloudStorageDirectory, backGroundTask.delocalizeBackgroundProcess, serverStream.drain)
        case x: AppConfig.Azure =>
          List(
            flushCache,
            backGroundTask.syncCloudStorageDirectory,
            backGroundTask.updateStorageAlg(x, blockerBound, storageAlgRef),
            serverStream
          )
      }
    }
  }

}
