package org.broadinstitute.dsp.workbench.welder
package server

import java.io.File
import java.nio.file.{Path, Paths}

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import io.circe.{Json, parser}
import _root_.fs2.{Stream, io, text}
import org.broadinstitute.dsde.workbench.google2.mock.{BaseFakeGoogleStorage, FakeGoogleStorageInterpreter}
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName, GetMetadataResponse, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

class ObjectServiceSpec extends FlatSpec with WelderTestSuite {
  val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map.empty)
  val metaCache = Ref.unsafe[IO, Map[Path, AdaptedGcsMetadataCache]](Map.empty)
  val objectServiceConfig = ObjectServiceConfig(Paths.get("/tmp"), WorkbenchEmail("me@gmail.com"), 20 minutes)
  val defaultGoogleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), FakeGoogleStorageInterpreter)
  val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
  val objectService = ObjectService(objectServiceConfig, defaultGoogleStorageAlg, global, storageLinkAlg, metaCache)

  "ObjectService" should "be able to localize a file" in {
    forAll {(bucketName: GcsBucketName, blobName: GcsBlobName, bodyString: String, localFileDestination: Path) => //Use string here just so it's easier to debug
      val body = bodyString.getBytes()
      // It would be nice to test objects with `/` in its name, but google storage emulator doesn't support it
      val requestBody =
        s"""
           |{
           |  "action": "localize",
           |  "entries": [
           |   {
           |     "sourceUri": "gs://${bucketName.value}/${blobName.value}",
           |     "localDestinationPath": "${localFileDestination}"
           |   }
           |  ]
           |}
      """.stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)

      val localAbsoluteFilePath = Paths.get(s"/tmp/${localFileDestination}")
      val res = for {
        _ <- FakeGoogleStorageInterpreter.removeObject(bucketName, blobName).compile.drain
        _ <- FakeGoogleStorageInterpreter.createBlob(bucketName, blobName, body, "text/plain", Map.empty, None, None).compile.drain
        resp <- objectService.service.run(request).value
        localFileBody <- io.file.readAll[IO](localAbsoluteFilePath, global, 4096)
          .compile
          .toList
        _ <- IO((new File(localFileDestination.toString)).delete())
      } yield {
        resp.get.status shouldBe (Status.NoContent)
        localFileBody should contain theSameElementsAs (body)
      }

      res.unsafeRunSync()
    }
  }

  it should "should be able to localize data uri" in {
    val localFileDestination = arbFilePath.arbitrary.sample.get
    val requestBody =
      s"""
         |{
         |  "action": "localize",
         |  "entries": [
         |   {
         |     "sourceUri": "data:application/json;base64,eyJkZXN0aW5hdGlvbiI6ICJnczovL2J1Y2tldC9ub3RlYm9va3MiLCAicGF0dGVybiI6ICJcLmlweW5iJCJ9",
         |     "localDestinationPath": "${localFileDestination}"
         |   }
         |  ]
         |}
      """.stripMargin

    val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
    val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)

    val expectedBody = """{"destination": "gs://bucket/notebooks", "pattern": "\.ipynb$"}""".stripMargin
    val localAbsoluteFilePath = Paths.get(s"/tmp/${localFileDestination}")
    val res = for {
      resp <- objectService.service.run(request).value
      localFileBody <- io.file.readAll[IO](localAbsoluteFilePath, global, 4096).through(fs2.text.utf8Decode).compile.foldMonoid
      _ <- IO((new File(localAbsoluteFilePath.toString)).delete())
    } yield {
      resp.get.status shouldBe (Status.NoContent)
      localFileBody shouldBe(expectedBody)
    }

    res.unsafeRunSync()
  }

  "/GET metadata" should "return no storage link is found if storagelink isn't found" in {
    forAll {
      (localFileDestination: Path) =>
        val requestBody = s"""
             |{
             |  "localPath": "${localFileDestination.toString}"
             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)

        val res = for {
          resp <- objectService.service.run(request).value
        } yield ()
        res.attempt.unsafeRunSync() shouldBe(Left(StorageLinkNotFoundException(s"No storage link found for ${localFileDestination.toString}")))
    }
  }

  it should "return RemoteNotFound if metadata is not found in GCS" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, defaultGoogleStorageAlg, global, storageLinkAlg, metaCache)

        val requestBody = s"""
                             |{
                             |  "localPath": "${localBaseDirectory.path.toString}/test.ipynb"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"REMOTE_NOT_FOUND","storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        val res = for {
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SafeMode if storagelink exists in LocalSafeBaseDirectory" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localSafeDirectory.path -> storageLink))
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, defaultGoogleStorageAlg, global, storageLinkAlg, metaCache)

        val requestBody =
          s"""
             |{
             |  "localPath": "${localSafeDirectory.path.toString}/test.ipynb"
             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"SAFE","storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        val res = for {
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe (expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.LIVE if crc32c matches" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 0L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)
        val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), storageService)
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, googleStorageAlg, global, storageLinkAlg, metaCache)

        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"LIVE","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.LocalChanged if crc32c doesn't match but local cached generation matches remote" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val metaCache = Ref.unsafe[IO, Map[Path, AdaptedGcsMetadataCache]](Map(Paths.get(localPath) -> AdaptedGcsMetadataCache(RelativePath(Paths.get(localPath)), None, Crc32("asdf"), 111L)))
        val bodyBytes = "this is great! Okay".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 111L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)
        val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), storageService)
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, googleStorageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"LOCAL_CHANGED","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.RemoteChanged if crc32c doesn't match and local cached generation doesn't match remote" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val metaCache = Ref.unsafe[IO, Map[Path, AdaptedGcsMetadataCache]](Map(Paths.get(localPath) -> AdaptedGcsMetadataCache(RelativePath(Paths.get(localPath)), None, Crc32("asdf"), 0L)))
        val bodyBytes = "this is great! Okay".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 1L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)
        val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), storageService)
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, googleStorageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"REMOTE_CHANGED","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.OutOfSync if crc32c doesn't match and local cached generation doesn't match remote" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great! Okay".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 1L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)
        val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), storageService)
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)

        val objectService = ObjectService(objectServiceConfig, googleStorageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"DESYNCHRONIZED","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return lastLockedBy if metadata shows it's locked by someone" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map("lastLockedBy" -> lockedBy.value, "lockExpiresAt" -> Long.MaxValue.toString), 0L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)
        val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), storageService)
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)

        val objectService = ObjectService(objectServiceConfig, googleStorageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"LIVE","lastLockedBy":"${lockedBy.value}","storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          println(body)
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return not return lastLockedBy if lock has expired" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map("lastLockedBy" -> lockedBy.value, "lockExpiresAt" -> Long.MinValue.toString), 0L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)
        val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), storageService)
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, googleStorageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBody = s"""{"syncMode":"EDIT","syncStatus":"LIVE","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}/${cloudStorageDirectory.blobPath.asString}","pattern":"*.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe(expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  "/safeDelocalize" should "delocalize a file" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metaCache = Ref.unsafe[IO, Map[Path, AdaptedGcsMetadataCache]](Map(Paths.get(localPath) -> AdaptedGcsMetadataCache(RelativePath(Paths.get(localPath)), None, Crc32("asdf"), 111L)))
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)

        val storageAlg = new GoogleStorageAlg {
          override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[Unit] = ???
          override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = ???
          override def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] = ???
          override def gcsToLocalFile(localAbsolutePath: Path, gsPath: GsPath, traceId: TraceId): Stream[IO, AdaptedGcsMetadata] = ???
          override def delocalize(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, traceId: TraceId): IO[DelocalizeResponse] = {
            IO(localObjectPath.asPath.toString shouldBe(localPath)) >>
            IO(gsPath.bucketName shouldBe(cloudStorageDirectory.bucketName)) >>
            IO(gsPath.blobName shouldBe(getFullBlobName(localBaseDirectory.path, Paths.get(localPath), cloudStorageDirectory.blobPath))) >>
            IO(generation shouldBe(111L)) >>
            IO(DelocalizeResponse(112L, Crc32("newHash")))
          }
        }
        val objectService = ObjectService(objectServiceConfig, storageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "action": "safeDelocalize",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)
        val res = for {
          resp <- objectService.service.run(request).value
        } yield {
          resp.get.status shouldBe Status.NoContent
        }
        res.unsafeRunSync()
    }
  }

  it should "delocalize file when it doesn't exist in metadata cache" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val storageAlg = new GoogleStorageAlg {
          override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[Unit] = ???
          override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = ???
          override def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] = ???
          override def gcsToLocalFile(localAbsolutePath: Path, gsPath: GsPath, traceId: TraceId): Stream[IO, AdaptedGcsMetadata] = ???
          override def delocalize(localObjectPath: RelativePath, gsPath: GsPath, generation: Long, traceId: TraceId): IO[DelocalizeResponse] = {
            IO(localObjectPath.asPath.toString shouldBe(localPath)) >>
              IO(gsPath.bucketName shouldBe(cloudStorageDirectory.bucketName)) >>
              IO(gsPath.blobName shouldBe(getFullBlobName(localBaseDirectory.path, Paths.get(localPath), cloudStorageDirectory.blobPath))) >>
              IO(generation shouldBe(0L)) >>
              IO(DelocalizeResponse(1L, Crc32("newHash")))
          }
        }
        val objectService = ObjectService(objectServiceConfig, storageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "action": "safeDelocalize",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)
        val res = for {
          resp <- objectService.service.run(request).value
        } yield {
          resp.get.status shouldBe Status.NoContent
        }
        res.unsafeRunSync()
    }
  }

  it should "do not delocalize a SAFE mode file" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localSafeDirectory.path -> storageLink))
        val localPath = s"${localSafeDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, defaultGoogleStorageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "action": "safeDelocalize",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localSafeDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath.toString)).delete())
          remoteFile <- FakeGoogleStorageInterpreter.getBlobBody(cloudStorageDirectory.bucketName, getFullBlobName(localSafeDirectory.path, Paths.get(localPath), cloudStorageDirectory.blobPath)).compile.toList
        } yield {
          resp shouldBe Left(SafeDelocalizeSafeModeFileError(s"${localPath} can't be delocalized since it's in safe mode"))
          remoteFile.isEmpty shouldBe(true)
        }
        res.unsafeRunSync()
    }
  }

  it should "throw GenerationMismatch exception if remote file has changed" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val metaCache = Ref.unsafe[IO, Map[Path, AdaptedGcsMetadataCache]](Map(Paths.get(localPath) -> AdaptedGcsMetadataCache(RelativePath(Paths.get(localPath)), None, Crc32("asdf"), 111L)))
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), GoogleStorageServiceWithFailures)
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, googleStorageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "action": "safeDelocalize",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp shouldBe Left(GenerationMismatch(s"Remote version has changed for /tmp/${localPath}. Generation mismatch"))
        }
        res.unsafeRunSync()
    }
  }

  "/delete" should "return StorageLinkNotFoundException" in {
    forAll {
      (localFileDestination: Path) =>
        val requestBody = s"""
                             |{
                             |  "action": "delete",
                             |  "localPath": "${localFileDestination.toString}"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)

        val res = for {
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8Decode).compile.foldMonoid
        } yield {
          resp.get.status shouldBe Status.Ok
        }
        res.attempt.unsafeRunSync() shouldBe(Left(StorageLinkNotFoundException(s"No storage link found for ${localFileDestination.toString}")))
    }
  }

  it should "return DeleteSafeModeFile" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localSafeDirectory.path -> storageLink))
        val localPath = s"${localSafeDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, defaultGoogleStorageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "action": "delete",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localSafeDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          resp <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath.toString)).delete())
          remoteFile <- FakeGoogleStorageInterpreter.getBlobBody(cloudStorageDirectory.bucketName, getFullBlobName(localSafeDirectory.path, Paths.get(localPath), cloudStorageDirectory.blobPath)).compile.toList
        } yield {
          resp shouldBe Left(DeleteSafeModeFileError(s"${localPath} can't be deleted since it's in safe mode"))
          remoteFile.isEmpty shouldBe(true)
        }
        res.unsafeRunSync()
    }
  }

  it should "return Delete a file from GCS" in {
    forAll {
      (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory, lockedBy: WorkbenchEmail) =>
        val storageLink = StorageLink(localBaseDirectory, localSafeDirectory, cloudStorageDirectory, "*.ipynb")
        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metaCache = Ref.unsafe[IO, Map[Path, AdaptedGcsMetadataCache]](Map(Paths.get(localPath) -> AdaptedGcsMetadataCache(RelativePath(Paths.get(localPath)), None, Crc32("asdf"), 111L)))
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, defaultGoogleStorageAlg, global, storageLinkAlg, metaCache)
        val requestBody = s"""
                             |{
                             |  "action": "delete",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/")).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val fullBlobName = getFullBlobName(localBaseDirectory.path, Paths.get(localPath), cloudStorageDirectory.blobPath)
        val res = for {
          _ <- Stream.emits(bodyBytes).covary[IO].through(fs2.io.file.writeAll[IO](Paths.get(s"/tmp/${localPath}"), global)).compile.drain //write to local file
          _ <- FakeGoogleStorageInterpreter.createBlob(cloudStorageDirectory.bucketName, fullBlobName, bodyBytes, "text/plain").compile.drain
          fileExisted <- FakeGoogleStorageInterpreter.getBlobBody(cloudStorageDirectory.bucketName, fullBlobName).compile.toList
          _ <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath.toString)).delete())
          remoteFile <- FakeGoogleStorageInterpreter.getBlobBody(cloudStorageDirectory.bucketName, fullBlobName).compile.toList
        } yield {
          fileExisted.nonEmpty shouldBe true
          remoteFile.isEmpty shouldBe(true)
        }
        res.unsafeRunSync()
    }
  }

  "acquireLock" should "should be able to acquire lock when no one holds the lock" in {
    forAll {
      (gsPath: GsPath, storageLink: StorageLink) =>
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val googleStorage = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), GoogleStorageServiceWithFailures)

        val storageLinksCache = Ref.unsafe[IO, Map[Path, StorageLink]](Map(storageLink.localBaseDirectory.path -> storageLink))
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val objectService = ObjectService(objectServiceConfig, defaultGoogleStorageAlg, global, storageLinkAlg, metaCache)
        val localAbsolutePath = Paths.get(s"/tmp/${storageLink.localBaseDirectory.path.toString}/test.ipynb")
        val requestBody = s"""
                             |{
                             |  "localPath": "${storageLink.localBaseDirectory.path.toString}/test.ipynb"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/lock")).withEntity[Json](requestBodyJson)

        // Create the local base directory
        val directory = new File(s"${localAbsolutePath.getParent.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val expectedBody = """{"result":"SUCCESS"}"""
        val res = for {
          res <- objectService.service.run(request).value
          body <- res.get.body.through(text.utf8Decode).compile.foldMonoid
        } yield {
          body shouldBe expectedBody
        }
        res.unsafeRunSync()
    }
  }
}

class FakeGoogleStorageService(metadataResponse: GetMetadataResponse) extends BaseFakeGoogleStorage {
  override def getObjectMetadata(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId]): fs2.Stream[IO, GetMetadataResponse] = Stream.emit(metadataResponse).covary[IO]
}

object FakeGoogleStorageService {
  def apply(metadata: GetMetadataResponse): FakeGoogleStorageService = new FakeGoogleStorageService(metadata)
}