package org.broadinstitute.dsp.workbench.welder
package server

import java.nio.file.Path
import java.util.concurrent.TimeUnit

import cats.effect.concurrent.Ref
import cats.effect.{ExitCode, IO, IOApp, Resource, Timer}
import cats.implicits._
import fs2.Stream
import io.chrisdavenport.linebacker.Linebacker
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Printer}
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
      googleStorageService <- Stream.resource(GoogleStorageService.fromApplicationDefault())
      storageLinksService = StorageLinksService(storageLinksCache)
      googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(appConfig.objectService.workingDirectory), googleStorageService)
      storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
      objectService = ObjectService(appConfig.objectService, googleStorageAlg, blockingEc, storageLinkAlg, metadataCache)
      serverStream = BlazeServerBuilder[IO].bindHttp(appConfig.serverPort, "0.0.0.0").withHttpApp(WelderApp(objectService, storageLinksService).service).serve
      cleanUpCache = (Stream.sleep(appConfig.cleanUpLockFrequency) ++ Stream.eval(cleanUpLock(metadataCache))).repeat
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

  private def cleanUpLock(metadataCache: MetadataCache)(implicit timer: Timer[IO], logger: Logger[IO]): IO[Unit] = {
    val res = for {
      now <- timer.clock.monotonic(TimeUnit.MILLISECONDS)
      updatedMap <- metadataCache.modify { mp =>
        val newMap = mp.map { kv =>
          val newLock = kv._2.lock match {
            case Some(l) =>
              if (l.lockExpiresAt.toEpochMilli < now)
                None
              else
                Some(l)
            case None => None
          }
          (kv._1 -> kv._2.copy(lock = newLock))
        }
        (newMap, newMap)
      }
      _ <- logger.info(s"updated metadata cache ${updatedMap.take(100)}")
    } yield ()
    res.handleErrorWith(t => logger.error(t)("fail to update metadata cache"))
  }

  private def cachedResource[A, B: Decoder: Encoder](path: Path, blockingEc: ExecutionContext, toTuple: B => List[(A, B)])(
      implicit logger: Logger[IO]
  ): Stream[IO, Ref[IO, Map[A, B]]] =
    for {
      cached <- readJsonFileToA[IO, List[B]](path).map(ls => ls.flatMap(b => toTuple(b)).toMap).handleErrorWith { error =>
        Stream.eval(logger.info(s"$path not found")) >> Stream.emit(Map.empty[A, B]).covary[IO]
      }
      ref <- Stream.resource(
        Resource.make(Ref.of[IO, Map[A, B]](cached))(
          ref =>
            Stream
              .eval(ref.get)
              .flatMap(x => Stream.emits(x.values.toSet.asJson.pretty(Printer.noSpaces).getBytes("UTF-8")))
              .through(fs2.io.file.writeAll(path, blockingEc))
              .compile
              .drain
        )
      )
    } yield ref

}
