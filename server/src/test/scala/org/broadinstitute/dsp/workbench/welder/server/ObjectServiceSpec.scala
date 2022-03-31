package org.broadinstitute.dsp.workbench.welder
package server

import _root_.fs2.{Pipe, Stream, text}
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.implicits._
import cats.mtl.Ask
import fs2.io.file.Files
import io.circe.{Decoder, Json, parser}
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.google2.{Crc32, GcsBlobName, GetMetadataResponse, GoogleStorageService, RemoveObjectResult}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.workbench.welder.Generators._
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.{LocalBaseDirectory, LocalSafeBaseDirectory}
import org.broadinstitute.dsp.workbench.welder.SourceUri.GsPath
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{Method, Request, Status, Uri}
import org.scalatest.flatspec.AnyFlatSpec
import org.typelevel.jawn.AsyncParser

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.Instant
import scala.concurrent.duration._
import scala.util.matching.Regex

class ObjectServiceSpec extends AnyFlatSpec with WelderTestSuite {
  val objectServiceConfig = ObjectServiceConfig(Paths.get("/tmp"), WorkbenchEmail("me@gmail.com"), 20 minutes)
  val objectService = initObjectService(Map.empty, Map.empty, None)

  "localize" should "be able to localize a file" in {
    forAll { (bucketName: GcsBucketName, blobName: GcsBlobName, bodyString: String, localFileDestination: Path) => //Use string here just so it's easier to debug
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
      val metadataCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
      val objectService = initObjectServiceWithMetadataCache(Map.empty, metadataCache, None)

      val res = for {
        _ <- FakeGoogleStorageInterpreter.removeObject(bucketName, blobName).compile.drain
        _ <- FakeGoogleStorageInterpreter.createBlob(bucketName, blobName, body, "text/plain", Map.empty, None, None).compile.drain
        resp <- objectService.service.run(request).value
        localFileBody <- Files[IO].readAll(fs2.io.file.Path.fromNioPath(localAbsoluteFilePath)).compile.toList
        _ <- IO((new File(localFileDestination.toString)).delete())
        metadata <- metadataCache.get
      } yield {
        resp.get.status shouldBe (Status.NoContent)
        localFileBody should contain theSameElementsAs (body)
        val relativePath = RelativePath(localFileDestination)
        val expectedCrc32 = Crc32c.calculateCrc32c(body)
        metadata shouldBe Map(
          relativePath -> AdaptedGcsMetadataCache(relativePath, RemoteState.Found(None, expectedCrc32), Some(0L))
        ) //generation is null from emulator. Somehow scala seems to translate that to 0L
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
      localFileBody <- Files[IO].readAll(fs2.io.file.Path.fromNioPath(localAbsoluteFilePath)).through(fs2.text.utf8.decode).compile.foldMonoid
      _ <- IO((new File(localAbsoluteFilePath.toString)).delete())
    } yield {
      resp.get.status shouldBe (Status.NoContent)
      localFileBody shouldBe (expectedBody)
    }

    res.unsafeRunSync()
  }

  it should "return 404 if trying to localize a non-existent file" in {
    forAll { (bucketName: GcsBucketName, blobName: GcsBlobName, bodyString: String, localFileDestination: Path) => //Use string here just so it's easier to debug
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
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/"), headers = fakeTraceIdHeader).withEntity[Json](requestBodyJson)

      val metadataCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
      val objectService = initObjectServiceWithMetadataCache(Map.empty, metadataCache, None)

      val res = for {
        resp <- objectService.service.run(request).value.attempt
      } yield {
        resp shouldBe Left(NotFoundException(fakeTraceId, s"gs://${bucketName.value}/${blobName.value} not found"))
      }

      res.unsafeRunSync()
    }
  }

  "checkMetadata" should "return no storage link is found if storagelink isn't found" in {
    forAll { (localFileDestination: Path) =>
      val requestBody = s"""
             |{
             |  "localPath": "${localFileDestination.toString}"
             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata"), headers = fakeTraceIdHeader).withEntity[Json](requestBodyJson)

      val res = for {
        _ <- objectService.service.run(request).value
      } yield ()
      res.attempt.unsafeRunSync() shouldBe (Left(StorageLinkNotFoundException(fakeTraceId, s"No storage link found for ${localFileDestination.toString}")))
    }
  }

  it should "return RemoteNotFound if metadata is not found in GCS" in {
    forAll { (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
      val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
      val objectService = initObjectService(Map(localBaseDirectory.path -> storageLink), Map.empty, None)

      val requestBody = s"""
                             |{
                             |  "localPath": "${localBaseDirectory.path.toString}/test.ipynb"
                             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
      val expectedBlobPath = cloudStorageDirectory.blobPath.fold("")(s => s"/${s.asString}")
      val expectedBody =
        s"""{"syncMode":"EDIT","syncStatus":"REMOTE_NOT_FOUND","storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}${expectedBlobPath}","pattern":"\\\\.ipynb"}}"""
      val res = for {
        resp <- objectService.service.run(request).value
        body <- resp.get.body.through(text.utf8.decode).compile.foldMonoid
      } yield {
        resp.get.status shouldBe Status.Ok
        body shouldBe (expectedBody)
      }
      res.unsafeRunSync()
    }
  }

  it should "return SafeMode if storagelink exists in LocalSafeBaseDirectory" in {
    forAll { (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
      val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
      val objectService = initObjectService(Map(localSafeDirectory.path -> storageLink), Map.empty, None)

      val requestBody =
        s"""
             |{
             |  "localPath": "${localSafeDirectory.path.toString}/test.ipynb"
             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
      val expectedBlobPath = cloudStorageDirectory.blobPath.fold("")(s => s"/${s.asString}")
      val expectedBody =
        s"""{"syncMode":"SAFE","storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}${expectedBlobPath}","pattern":"\\\\.ipynb"}}"""
      val res = for {
        resp <- objectService.service.run(request).value
        body <- resp.get.body.through(text.utf8.decode).compile.foldMonoid
      } yield {
        resp.get.status shouldBe Status.Ok
        body shouldBe (expectedBody)
      }
      res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.LIVE if crc32c matches" in {
    forAll { (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
      val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
      val bodyBytes = "this is great!".getBytes("UTF-8")
      val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 0L) //This crc32c is from gsutil
      val storageService = FakeGoogleStorageService(metadataResp)
      val metadataCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
      val objectService = initObjectServiceWithMetadataCache(Map(localBaseDirectory.path -> storageLink), metadataCache, Some(storageService))

      val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
      val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
      val expectedBlobPath = cloudStorageDirectory.blobPath.fold("")(s => s"/${s.asString}")
      val expectedBody =
        s"""{"syncMode":"EDIT","syncStatus":"LIVE","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}${expectedBlobPath}","pattern":"\\\\.ipynb"}}"""
      // Create the local base directory
      val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        _ <- Stream.emits(bodyBytes).covary[IO].through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}"))).compile.drain //write to local file
        resp <- objectService.service.run(request).value
        body <- resp.get.body.through(text.utf8.decode).compile.foldMonoid
        _ <- IO((new File(localPath.toString)).delete())
        metadata <- metadataCache.get
      } yield {
        resp.get.status shouldBe Status.Ok
        body shouldBe (expectedBody)
        val relativePath = RelativePath(Paths.get(localPath))
        metadata shouldBe Map(
          relativePath -> AdaptedGcsMetadataCache(relativePath, RemoteState.Found(None, Crc32("aZKdIw==")), None)
        ) //lock should be updated, but emulator doesn't support user defined metadata yet
      }
      res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.LocalChanged if crc32c doesn't match but local cached generation matches remote" in {
    forAll { (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
      val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
      val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localBaseDirectory.path -> storageLink))
      val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
      val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](
        Map(
          RelativePath(Paths.get(localPath)) -> AdaptedGcsMetadataCache(RelativePath(Paths.get(localPath)), RemoteState.Found(None, Crc32("asdf")), Some(111L))
        )
      )
      val bodyBytes = "this is great! Okay".getBytes("UTF-8")
      val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 111L) //This crc32c is from gsutil
      val storageService = FakeGoogleStorageService(metadataResp)
      val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), storageService)
      val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
      val metadataCacheAlg = new MetadataCacheInterp(metaCache)
      val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
      val objectService = ObjectService(permitsRef, objectServiceConfig, googleStorageAlg, storageLinkAlg, metadataCacheAlg)
      val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
      val expectedBlobPath = cloudStorageDirectory.blobPath.fold("")(s => s"/${s.asString}")
      val expectedBody =
        s"""{"syncMode":"EDIT","syncStatus":"LOCAL_CHANGED","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}${expectedBlobPath}","pattern":"\\\\.ipynb"}}"""
      // Create the local base directory
      val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        _ <- Stream.emits(bodyBytes).covary[IO].through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}"))).compile.drain //write to local file
        resp <- objectService.service.run(request).value
        body <- resp.get.body.through(text.utf8.decode).compile.foldMonoid
        _ <- IO((new File(localPath.toString)).delete())
      } yield {
        resp.get.status shouldBe Status.Ok
        body shouldBe (expectedBody)
      }
      res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.RemoteChanged if crc32c doesn't match and local cached generation doesn't match remote" in {
    forAll { (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
      val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
      val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localBaseDirectory.path -> storageLink))
      val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
      val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](
        Map(RelativePath(Paths.get(localPath)) -> AdaptedGcsMetadataCache(RelativePath(Paths.get(localPath)), RemoteState.Found(None, Crc32("asdf")), Some(0L)))
      )
      val bodyBytes = "this is great! Okay".getBytes("UTF-8")
      val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 1L) //This crc32c is from gsutil
      val storageService = FakeGoogleStorageService(metadataResp)
      val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), storageService)
      val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
      val metadataCacheAlg = new MetadataCacheInterp(metaCache)
      val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
      val objectService = ObjectService(permitsRef, objectServiceConfig, googleStorageAlg, storageLinkAlg, metadataCacheAlg)
      val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
      val expectedBlobPath = cloudStorageDirectory.blobPath.fold("")(s => s"/${s.asString}")
      val expectedBody =
        s"""{"syncMode":"EDIT","syncStatus":"REMOTE_CHANGED","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}${expectedBlobPath}","pattern":"\\\\.ipynb"}}"""
      // Create the local base directory
      val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        _ <- Stream.emits(bodyBytes).covary[IO].through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}"))).compile.drain //write to local file
        resp <- objectService.service.run(request).value
        body <- resp.get.body.through(text.utf8.decode).compile.foldMonoid
        _ <- IO((new File(localPath.toString)).delete())
      } yield {
        resp.get.status shouldBe Status.Ok
        body shouldBe (expectedBody)
      }
      res.unsafeRunSync()
    }
  }

  it should "return SyncStatus.OutOfSync if crc32c doesn't match and no local cached metadata found for the file" in {
    forAll { (cloudStorageDirectory: CloudStorageDirectory, localBaseDirectory: LocalBaseDirectory, localSafeDirectory: LocalSafeBaseDirectory) =>
      val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, """\.ipynb$""".stripMargin.r)
      val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
      val bodyBytes = "this is great! Okay".getBytes("UTF-8")
      val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map.empty, 1L) //This crc32c is from gsutil
      val storageService = FakeGoogleStorageService(metadataResp)

      val objectService = initObjectService(Map(localBaseDirectory.path -> storageLink), Map.empty, Some(storageService))
      val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
      val expectedBlobPath = cloudStorageDirectory.blobPath.fold("")(s => s"/${s.asString}")
      val expectedBody =
        s"""{"syncMode":"EDIT","syncStatus":"DESYNCHRONIZED","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}${expectedBlobPath}","pattern":"\\\\.ipynb$$"}}"""
      // Create the local base directory
      val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        _ <- Stream.emits(bodyBytes).covary[IO].through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}"))).compile.drain //write to local file
        resp <- objectService.service.run(request).value
        body <- resp.get.body.through(text.utf8.decode).compile.foldMonoid
        _ <- IO((new File(localPath.toString)).delete())
      } yield {
        resp.get.status shouldBe Status.Ok
        body shouldBe (expectedBody)
      }
      res.unsafeRunSync()
    }
  }

  it should "return lastLockedBy if metadata shows it's locked by someone" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory,
          lockedBy: WorkbenchEmail
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map("lastLockedBy" -> lockedBy.value, "lockExpiresAt" -> Long.MaxValue.toString), 0L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)

        val objectService = initObjectService(Map(localBaseDirectory.path -> storageLink), Map.empty, Some(storageService))
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBlobPath = cloudStorageDirectory.blobPath.fold("")(s => s"/${s.asString}")
        val expectedBody =
          s"""{"syncMode":"EDIT","syncStatus":"LIVE","lastLockedBy":"${lockedBy.value}","storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}${expectedBlobPath}","pattern":"\\\\.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8.decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe (expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  it should "return not return lastLockedBy if lock has expired" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory,
          lockedBy: WorkbenchEmail
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metadataResp = GetMetadataResponse.Metadata(Crc32("aZKdIw=="), Map("lastLockedBy" -> lockedBy.value, "lockExpiresAt" -> Long.MinValue.toString), 0L) //This crc32c is from gsutil
        val storageService = FakeGoogleStorageService(metadataResp)
        val objectService = initObjectService(Map(localBaseDirectory.path -> storageLink), Map.empty, Some(storageService))
        val requestBody = s"""
                             |{
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/metadata")).withEntity[Json](requestBodyJson)
        val expectedBlobPath = cloudStorageDirectory.blobPath.fold("")(s => s"/${s.asString}")
        val expectedBody =
          s"""{"syncMode":"EDIT","syncStatus":"LIVE","lastLockedBy":null,"storageLink":{"localBaseDirectory":"${localBaseDirectory.path.toString}","localSafeModeBaseDirectory":"${localSafeDirectory.path.toString}","cloudStorageDirectory":"gs://${cloudStorageDirectory.bucketName}${expectedBlobPath}","pattern":"\\\\.ipynb"}}"""
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value
          body <- resp.get.body.through(text.utf8.decode).compile.foldMonoid
          _ <- IO((new File(localPath.toString)).delete())
        } yield {
          resp.get.status shouldBe Status.Ok
          body shouldBe (expectedBody)
        }
        res.unsafeRunSync()
    }
  }

  "/safeDelocalize" should "delocalize a file" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory,
          lockedBy: WorkbenchEmail
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](
          Map(
            RelativePath(Paths.get(localPath)) -> AdaptedGcsMetadataCache(
              RelativePath(Paths.get(localPath)),
              RemoteState.Found(None, Crc32("asdf")),
              Some(111L)
            )
          )
        ) //this crc32c just needs to be something different from the real file on disk so that we are faking there's some local change
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)

        val storageAlg = new MockGoogleStorageAlg {
          override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
            IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
          override def delocalize(
              localObjectPath: RelativePath,
              gsPath: GsPath,
              generation: Long,
              userDefinedMeta: Map[String, String],
              traceId: TraceId
          ): IO[DelocalizeResponse] =
            IO(localObjectPath.asPath.toString shouldBe (localPath)) >>
              IO(gsPath.bucketName shouldBe (cloudStorageDirectory.bucketName)) >>
              IO(gsPath.blobName shouldBe (getFullBlobName(localBaseDirectory.path, Paths.get(localPath), cloudStorageDirectory.blobPath))) >>
              IO(generation shouldBe (111L)) >>
              IO(DelocalizeResponse(112L, Crc32("newHash")))
          override def localizeCloudDirectory(
              localBaseDirectory: RelativePath,
              cloudStorageDirectory: CloudStorageDirectory,
              workingDir: Path,
              pattern: Regex,
              traceId: TraceId
          ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
          override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
          override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
        }
        val metadataCacheAlg = new MetadataCacheInterp(metaCache)
        val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
        val objectService = ObjectService(permitsRef, objectServiceConfig, storageAlg, storageLinkAlg, metadataCacheAlg)
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
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value
          metadata <- metaCache.get
        } yield {
          resp.get.status shouldBe Status.NoContent
          val relativePath = RelativePath(Paths.get(localPath))
          val cache = metadata.get(relativePath).getOrElse(throw new Exception("invalid state"))
          cache.localFileGeneration shouldBe Some(112L)
          cache.remoteState.asInstanceOf[RemoteState.Found].crc32c shouldBe Crc32("newHash")
        }
        res.unsafeRunSync()
    }
  }

  it should "not delocalize a file if it doesn't satisfy storagelink's pattern" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.txt"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](
          Map(
            RelativePath(Paths.get(localPath)) -> AdaptedGcsMetadataCache(
              RelativePath(Paths.get(localPath)),
              RemoteState.Found(None, Crc32("asdf")),
              Some(111L)
            )
          )
        ) //this crc32c just needs to be something different from the real file on disk so that we are faking there's some local change
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)

        val storageAlg = new MockGoogleStorageAlg {
          override def delocalize(
              localObjectPath: RelativePath,
              gsPath: GsPath,
              generation: Long,
              userDefinedMeta: Map[String, String],
              traceId: TraceId
          ): IO[DelocalizeResponse] = IO.raiseError(fail("delocalize shouldn't happen"))
        }
        val metadataCacheAlg = new MetadataCacheInterp(metaCache)
        val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
        val objectService = ObjectService(permitsRef, objectServiceConfig, storageAlg, storageLinkAlg, metadataCacheAlg)
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
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value
        } yield {
          resp.get.status shouldBe Status.NoContent
        }
        res.unsafeRunSync()
    }
  }

  it should "delocalize a file even if remote copy is deleted" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory,
          lockedBy: WorkbenchEmail
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](
          Map(RelativePath(Paths.get(localPath)) -> AdaptedGcsMetadataCache(RelativePath(Paths.get(localPath)), RemoteState.NotFound, Some(111L)))
        ) //this crc32c just needs to be something different from the real file on disk so that we are faking there's some local change
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)

        val storageAlg = new MockGoogleStorageAlg {
          override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
            IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
          override def delocalize(
              localObjectPath: RelativePath,
              gsPath: GsPath,
              generation: Long,
              userDefinedMeta: Map[String, String],
              traceId: TraceId
          ): IO[DelocalizeResponse] =
            IO(generation shouldBe (0L)) >> // even thought generation in local cache is not 111L, we delocalize the file with generation being 0L to recreate the file
              IO(DelocalizeResponse(112L, Crc32("newHash")))
          override def localizeCloudDirectory(
              localBaseDirectory: RelativePath,
              cloudStorageDirectory: CloudStorageDirectory,
              workingDir: Path,
              pattern: Regex,
              traceId: TraceId
          ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
          override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
          override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
        }
        val metadataCacheAlg = new MetadataCacheInterp(metaCache)
        val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
        val objectService = ObjectService(permitsRef, objectServiceConfig, storageAlg, storageLinkAlg, metadataCacheAlg)
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
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value
        } yield {
          resp.get.status shouldBe Status.NoContent
        }
        res.unsafeRunSync()
    }
  }

  it should "not delocalize a file if its crc32c is the same as remote" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory,
          lockedBy: WorkbenchEmail
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val cachedCrc32c = Crc32c.calculateCrc32c(bodyBytes)
        val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](
          Map(
            RelativePath(Paths.get(localPath)) -> AdaptedGcsMetadataCache(RelativePath(Paths.get(localPath)), RemoteState.Found(None, cachedCrc32c), Some(111L))
          )
        )
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)

        val storageAlg = new MockGoogleStorageAlg {
          override def localizeCloudDirectory(
              localBaseDirectory: RelativePath,
              cloudStorageDirectory: CloudStorageDirectory,
              workingDir: Path,
              pattern: Regex,
              traceId: TraceId
          ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
          override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
            IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
          override def delocalize(
              localObjectPath: RelativePath,
              gsPath: GsPath,
              generation: Long,
              userDefinedMeta: Map[String, String],
              traceId: TraceId
          ): IO[DelocalizeResponse] =
            IO.raiseError(new Exception("delocalize shouldn't happen when crc32c hasn't changed"))
          override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
          override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
        }
        val metadataCacheAlg = new MetadataCacheInterp(metaCache)
        val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
        val objectService = ObjectService(permitsRef, objectServiceConfig, storageAlg, storageLinkAlg, metadataCacheAlg)
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
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value
          metadata <- metaCache.get
        } yield {
          resp.get.status shouldBe Status.NoContent
        }
        res.unsafeRunSync()
    }
  }

  it should "be able to delocalize a new file" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory,
          lockedBy: WorkbenchEmail
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val storageAlg = new MockGoogleStorageAlg {
          override def localizeCloudDirectory(
              localBaseDirectory: RelativePath,
              cloudStorageDirectory: CloudStorageDirectory,
              workingDir: Path,
              pattern: Regex,
              traceId: TraceId
          ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
          override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
            IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
          override def delocalize(
              localObjectPath: RelativePath,
              gsPath: GsPath,
              generation: Long,
              userDefinedMeta: Map[String, String],
              traceId: TraceId
          ): IO[DelocalizeResponse] =
            IO(localObjectPath.asPath.toString shouldBe (localPath)) >>
              IO(gsPath.bucketName shouldBe (cloudStorageDirectory.bucketName)) >>
              IO(gsPath.blobName shouldBe (getFullBlobName(localBaseDirectory.path, Paths.get(localPath), cloudStorageDirectory.blobPath))) >>
              IO(generation shouldBe (0L)) >>
              IO(DelocalizeResponse(112L, Crc32("newHash")))
          override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
          override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
        }
        val objectService = initObjectServiceWithGoogleStorageAlg(Map(localBaseDirectory.path -> storageLink), Map.empty, storageAlg)
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
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value
        } yield {
          resp.get.status shouldBe (Status.NoContent)
        }
        res.unsafeRunSync()
    }
  }

  it should "do not delocalize a SAFE mode file" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory,
          lockedBy: WorkbenchEmail
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val localPath = s"${localSafeDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val objectService = initObjectService(Map(localSafeDirectory.path -> storageLink), Map.empty, None)
        val requestBody = s"""
                             |{
                             |  "action": "safeDelocalize",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/"), headers = fakeTraceIdHeader).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localSafeDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath.toString)).delete())
          remoteFile <- FakeGoogleStorageInterpreter
            .getBlobBody(cloudStorageDirectory.bucketName, getFullBlobName(localSafeDirectory.path, Paths.get(localPath), cloudStorageDirectory.blobPath))
            .compile
            .toList
        } yield {
          resp shouldBe Left(SafeDelocalizeSafeModeFileError(fakeTraceId, s"${localPath} can't be delocalized since it's in safe mode"))
          remoteFile.isEmpty shouldBe (true)
        }
        res.unsafeRunSync()
    }
  }

  it should "throw GenerationMismatch exception if remote file has changed" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map(localBaseDirectory.path -> storageLink))
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](
          Map(
            RelativePath(Paths.get(localPath)) -> AdaptedGcsMetadataCache(
              RelativePath(Paths.get(localPath)),
              RemoteState.Found(None, Crc32("asdf")),
              Some(111L)
            )
          )
        )
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val googleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), GoogleStorageServiceWithFailures)
        val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
        val metadataCacheAlg = new MetadataCacheInterp(metaCache)
        val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
        val objectService = ObjectService(permitsRef, objectServiceConfig, googleStorageAlg, storageLinkAlg, metadataCacheAlg)
        val requestBody = s"""
                             |{
                             |  "action": "safeDelocalize",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/"), headers = fakeTraceIdHeader).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localBaseDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath)).delete())
        } yield {
          resp shouldBe Left(
            GenerationMismatch(fakeTraceId, s"Remote version has changed for /tmp/${localPath}. Generation mismatch (local generation: 111). null")
          )
        }
        res.unsafeRunSync()
    }
  }

  "/delete" should "return StorageLinkNotFoundException" in {
    forAll { (localFileDestination: Path) =>
      val requestBody = s"""
                             |{
                             |  "action": "delete",
                             |  "localPath": "${localFileDestination.toString}"
                             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/"), headers = fakeTraceIdHeader).withEntity[Json](requestBodyJson)

      val res = for {
        resp <- objectService.service.run(request).value
      } yield {
        resp.get.status shouldBe Status.Ok
      }
      res.attempt.unsafeRunSync() shouldBe (Left(StorageLinkNotFoundException(fakeTraceId, s"No storage link found for ${localFileDestination.toString}")))
    }
  }

  it should "return DeleteSafeModeFile" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory,
          lockedBy: WorkbenchEmail
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val localPath = s"${localSafeDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val objectService = initObjectService(Map(localSafeDirectory.path -> storageLink), Map.empty, None)
        val requestBody = s"""
                             |{
                             |  "action": "delete",
                             |  "localPath": "$localPath"
                             |}""".stripMargin
        val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
        val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/"), headers = fakeTraceIdHeader).withEntity[Json](requestBodyJson)
        // Create the local base directory
        val directory = new File(s"/tmp/${localSafeDirectory.path.toString}")
        if (!directory.exists) {
          directory.mkdirs
        }
        val res = for {
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          resp <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath.toString)).delete())
          remoteFile <- FakeGoogleStorageInterpreter
            .getBlobBody(cloudStorageDirectory.bucketName, getFullBlobName(localSafeDirectory.path, Paths.get(localPath), cloudStorageDirectory.blobPath))
            .compile
            .toList
        } yield {
          resp shouldBe Left(DeleteSafeModeFileError(fakeTraceId, s"${localPath} can't be deleted since it's in safe mode"))
          remoteFile.isEmpty shouldBe (true)
        }
        res.unsafeRunSync()
    }
  }

  it should "return Delete a file from GCS" in {
    forAll {
      (
          cloudStorageDirectory: CloudStorageDirectory,
          localBaseDirectory: LocalBaseDirectory,
          localSafeDirectory: LocalSafeBaseDirectory,
          lockedBy: WorkbenchEmail
      ) =>
        val storageLink = StorageLink(localBaseDirectory, Some(localSafeDirectory), cloudStorageDirectory, "\\.ipynb".r)
        val localPath = s"${localBaseDirectory.path.toString}/test.ipynb"
        val bodyBytes = "this is great!".getBytes("UTF-8")
        val metadataCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](
          Map(
            RelativePath(Paths.get(localPath)) -> AdaptedGcsMetadataCache(
              RelativePath(Paths.get(localPath)),
              RemoteState.Found(None, Crc32("asdf")),
              Some(111L)
            )
          )
        )
        val objectService = initObjectServiceWithMetadataCache(Map(localBaseDirectory.path -> storageLink), metadataCache, None)
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
          _ <- Stream
            .emits(bodyBytes)
            .covary[IO]
            .through(Files[IO].writeAll(fs2.io.file.Path(s"/tmp/${localPath}")))
            .compile
            .drain //write to local file
          _ <- FakeGoogleStorageInterpreter.createBlob(cloudStorageDirectory.bucketName, fullBlobName, bodyBytes, "text/plain").compile.drain
          fileExisted <- FakeGoogleStorageInterpreter.getBlobBody(cloudStorageDirectory.bucketName, fullBlobName).compile.toList
          _ <- objectService.service.run(request).value.attempt
          _ <- IO((new File(localPath)).delete())
          remoteFile <- FakeGoogleStorageInterpreter.getBlobBody(cloudStorageDirectory.bucketName, fullBlobName).compile.toList
          meta <- metadataCache.get
        } yield {
          fileExisted.nonEmpty shouldBe true
          remoteFile.isEmpty shouldBe (true)
          val relativePath = RelativePath(Paths.get(localPath))
          meta.get(relativePath) shouldBe (None)
        }
        res.unsafeRunSync()
    }
  }

  "acquireLock" should "should not be able to acquire lock object doesn't exist in GCS" in {
    forAll { (storageLink: StorageLink) =>
      val objectService = initObjectService(Map(storageLink.localBaseDirectory.path -> storageLink), Map.empty, None) //ObjectService(objectServiceConfig, defaultGoogleStorageAlg, global, storageLinkAlg, metaCache)
      val localAbsolutePath = Paths.get(s"/tmp/${storageLink.localBaseDirectory.path.toString}/test.ipynb")
      val requestBody = s"""
                             |{
                             |  "localPath": "${storageLink.localBaseDirectory.path.toString}/test.ipynb"
                             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/lock"), headers = fakeTraceIdHeader).withEntity[Json](requestBodyJson)

      // Create the local base directory
      val directory = new File(s"${localAbsolutePath.getParent.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        r <- objectService.service.run(request).value.attempt
      } yield {
        val fullBlobPath = getFullBlobName(
          storageLink.localBaseDirectory.path,
          storageLink.localBaseDirectory.path.asPath.resolve("test.ipynb"),
          storageLink.cloudStorageDirectory.blobPath
        )
        val gsPath = GsPath(storageLink.cloudStorageDirectory.bucketName, fullBlobPath)
        r shouldBe Left(NotFoundException(fakeTraceId, s"${gsPath} not found in Google Storage"))
      }
      res.unsafeRunSync()
    }
  }

  it should "update metadata cache when lock is updated" in {
    forAll { (storageLink: StorageLink) =>
      val metadataCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
      val objectService = initObjectServiceWithMetadataCache(Map(storageLink.localBaseDirectory.path -> storageLink), metadataCache, None)
      val localAbsolutePath = Paths.get(s"/tmp/${storageLink.localBaseDirectory.path.toString}/test.ipynb")
      val requestBody = s"""
                             |{
                             |  "localPath": "${storageLink.localBaseDirectory.path.toString}/test.ipynb"
                             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/lock"), headers = fakeTraceIdHeader).withEntity[Json](requestBodyJson)

      // Create the local base directory
      val directory = new File(s"${localAbsolutePath.getParent.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val bodyBytes = "this is great!".getBytes("UTF-8")
      val fullBlobPath = getFullBlobName(
        storageLink.localBaseDirectory.path,
        storageLink.localBaseDirectory.path.asPath.resolve("test.ipynb"),
        storageLink.cloudStorageDirectory.blobPath
      )

      val res = for {
        _ <- FakeGoogleStorageInterpreter
          .createBlob(storageLink.cloudStorageDirectory.bucketName, fullBlobPath, bodyBytes, "text/plain", Map.empty, None, None)
          .compile
          .drain
        now <- IO.realTimeInstant
        _ <- objectService.service.run(request).value
        metadata <- metadataCache.get
      } yield {
        val cache = metadata.get(RelativePath(Paths.get(s"${storageLink.localBaseDirectory.path.toString}/test.ipynb"))).get
        val remoteState = cache.remoteState.asInstanceOf[RemoteState.Found]
        (remoteState.lock.get.lockExpiresAt.toEpochMilli - now.toEpochMilli > 0) shouldBe (true)
      }
      res.unsafeRunSync()
    }
  }

  it should "should be able to acquire lock when no one holds the lock" in {
    forAll { (storageLink: StorageLink) =>
      val bodyBytes = "this is great!".getBytes("UTF-8")

      val objectService = initObjectService(Map(storageLink.localBaseDirectory.path -> storageLink), Map.empty, None)
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
      val fullBlobPath = getFullBlobName(
        storageLink.localBaseDirectory.path,
        storageLink.localBaseDirectory.path.asPath.resolve("test.ipynb"),
        storageLink.cloudStorageDirectory.blobPath
      )
      val res = for {
        _ <- FakeGoogleStorageInterpreter
          .createBlob(storageLink.cloudStorageDirectory.bucketName, fullBlobPath, bodyBytes, "text/plain", Map.empty, None, None)
          .compile
          .drain
        res <- objectService.service.run(request).value
        _ <- FakeGoogleStorageInterpreter
          .getBlob(storageLink.cloudStorageDirectory.bucketName, fullBlobPath, None)
          .compile
          .lastOrError //Make sure the blob exists
      } yield {
        res.get.status shouldBe (Status.NoContent)
      }
      res.unsafeRunSync()
    }
  }

  it should "should be able to acquire lock when lock is owned by current user" in {
    forAll { (storageLink: StorageLink) =>
      val bodyBytes = "this is great!".getBytes("UTF-8")
      val googleStorageAlg = new MockGoogleStorageAlg {
        override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
          IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
        override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] =
          for {
            now <- IO.realTimeInstant
            hashedLockedBy <- IO.fromEither(hashString(lockedByString(gsPath.bucketName, objectServiceConfig.ownerEmail)))
          } yield Some(AdaptedGcsMetadata(Some(Lock(hashedLockedBy, Instant.ofEpochMilli(now.toEpochMilli + 5.minutes.toMillis))), Crc32("fakecrc32"), 0L))
        override def localizeCloudDirectory(
            localBaseDirectory: RelativePath,
            cloudStorageDirectory: CloudStorageDirectory,
            workingDir: Path,
            pattern: Regex,
            traceId: TraceId
        ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
        override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
        override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
      }
      val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
      val objectService =
        initObjectServiceWithMetadataCacheAndGoogleStorageAlg(Map(storageLink.localBaseDirectory.path -> storageLink), metaCache, googleStorageAlg)
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
      val fullBlobPath = getFullBlobName(
        storageLink.localBaseDirectory.path,
        storageLink.localBaseDirectory.path.asPath.resolve("test.ipynb"),
        storageLink.cloudStorageDirectory.blobPath
      )
      val res = for {
        res <- objectService.service.run(request).value
        cache <- metaCache.get
      } yield {
        res.get.status shouldBe (Status.NoContent)
      }
      res.unsafeRunSync()
    }
  }

  it should "not be able to acquire lock when lock is owned by some other user and it hasn't expired" in {
    forAll { (storageLink: StorageLink) =>
      val bodyBytes = "this is great!".getBytes("UTF-8")
      val googleStorageAlg = new MockGoogleStorageAlg {
        override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
          IO.pure(UpdateMetadataResponse.DirectMetadataUpdate)
        override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] =
          for {
            hashedLockedBy <- IO.fromEither(hashString(lockedByString(gsPath.bucketName, WorkbenchEmail("someoneElse@gmail.com"))))
            now <- IO.realTimeInstant
          } yield Some(AdaptedGcsMetadata(Some(Lock(hashedLockedBy, Instant.ofEpochMilli(now.toEpochMilli + 5.minutes.toMillis))), Crc32("fakecrc32"), 0L))
        override def localizeCloudDirectory(
            localBaseDirectory: RelativePath,
            cloudStorageDirectory: CloudStorageDirectory,
            workingDir: Path,
            pattern: Regex,
            traceId: TraceId
        ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
        override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
        override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
      }
      val objectService = initObjectServiceWithGoogleStorageAlg(Map(storageLink.localBaseDirectory.path -> storageLink), Map.empty, googleStorageAlg)
      val localAbsolutePath = Paths.get(s"/tmp/${storageLink.localBaseDirectory.path.toString}/test.ipynb")
      val requestBody = s"""
                             |{
                             |  "localPath": "${storageLink.localBaseDirectory.path.toString}/test.ipynb"
                             |}""".stripMargin
      val requestBodyJson = parser.parse(requestBody).getOrElse(throw new Exception(s"invalid request body $requestBody"))
      val request = Request[IO](method = Method.POST, uri = Uri.unsafeFromString("/lock"), headers = fakeTraceIdHeader).withEntity[Json](requestBodyJson)

      // Create the local base directory
      val directory = new File(s"${localAbsolutePath.getParent.toString}")
      if (!directory.exists) {
        directory.mkdirs
      }
      val res = for {
        res <- objectService.service.run(request).value.attempt
      } yield {
        res shouldBe (Left(LockedByOther(fakeTraceId, s"lock is already acquired by someone else")))
      }
      res.unsafeRunSync()
    }
  }

  it should "should be able to acquire lock when lock is owned by another user and it can't update metadata directly" in {
    forAll { (storageLink: StorageLink) =>
      val googleStorageAlg = new MockGoogleStorageAlg {
        override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] =
          IO.pure(UpdateMetadataResponse.ReUploadObject(1L, Crc32("newcrc32")))
        override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] =
          for {
            now <- IO.realTimeInstant
            hashedLockedBy <- IO.fromEither(hashString(lockedByString(gsPath.bucketName, objectServiceConfig.ownerEmail)))
          } yield Some(AdaptedGcsMetadata(Some(Lock(hashedLockedBy, Instant.ofEpochMilli(now.toEpochMilli + 5.minutes.toMillis))), Crc32("fakecrc32"), 0L))
        override def localizeCloudDirectory(
            localBaseDirectory: RelativePath,
            cloudStorageDirectory: CloudStorageDirectory,
            workingDir: Path,
            pattern: Regex,
            traceId: TraceId
        ): Stream[IO, AdaptedGcsMetadataCache] = Stream.empty
        override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
        override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
      }
      val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
      val objectService =
        initObjectServiceWithMetadataCacheAndGoogleStorageAlg(Map(storageLink.localBaseDirectory.path -> storageLink), metaCache, googleStorageAlg)
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
      val res = for {
        res <- objectService.service.run(request).value
        cache <- metaCache.get
        expectedHashedLockedBy <- IO.fromEither(hashString(lockedByString(storageLink.cloudStorageDirectory.bucketName, objectServiceConfig.ownerEmail)))
      } yield {
        res.get.status shouldBe (Status.NoContent)
        val localPath = RelativePath(Paths.get(s"${storageLink.localBaseDirectory.path.toString}/test.ipynb"))
        val actualCache = cache.get(localPath).get
        val actualRemoteState = actualCache.remoteState.asInstanceOf[RemoteState.Found]
        actualRemoteState.lock.get.hashedLockedBy shouldBe (expectedHashedLockedBy)
        actualRemoteState.crc32c shouldBe Crc32("newcrc32")
        actualCache.localFileGeneration shouldBe (Some(1L))
      }
      res.unsafeRunSync()
    }
  }

  "preventConcurrentAction" should "be able to prevent two IOs run concurrently" in {
    val localPath = genRelativePath.sample.get
    val objectService = initObjectServiceWithPermits(Map.empty)
    val io1 = IO.sleep(4 seconds) >> IO(println("finished 1"))
    val io2 = IO.sleep(1 seconds) >> IO(println("finished 2"))
    val res = for {
      start <- IO.realTimeInstant
      _ = objectService.preventConcurrentAction(io1, localPath).unsafeToFuture() //start io1 asynchronously
      _ <- IO.sleep(500 millis) // adding this tiny sleep here so that at least there's enough time for lock to kick in
      _ <- objectService.preventConcurrentAction(io2, localPath)
      end <- IO.realTimeInstant
    } yield {
      val duration = end.toEpochMilli - start.toEpochMilli
      duration should be > 3000L
    }
    res.unsafeRunSync()
  }

  private def initObjectService(
      storageLinks: Map[RelativePath, StorageLink],
      metadata: Map[RelativePath, AdaptedGcsMetadataCache],
      googleStorageService: Option[GoogleStorageService[IO]]
  ): ObjectService = {
    val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
    val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](storageLinks)
    val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](metadata)
    val defaultGoogleStorageAlg =
      GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), googleStorageService.getOrElse(FakeGoogleStorageInterpreter))
    val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
    val metadataCacheAlg = new MetadataCacheInterp(metaCache)
    ObjectService(permitsRef, objectServiceConfig, defaultGoogleStorageAlg, storageLinkAlg, metadataCacheAlg)
  }

  private def initObjectServiceWithMetadataCache(
      storageLinks: Map[RelativePath, StorageLink],
      metadata: Ref[IO, Map[RelativePath, AdaptedGcsMetadataCache]],
      googleStorageService: Option[GoogleStorageService[IO]]
  ): ObjectService = {
    val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
    val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](storageLinks)
    val defaultGoogleStorageAlg =
      GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), googleStorageService.getOrElse(FakeGoogleStorageInterpreter))
    val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
    val metadataCacheAlg = new MetadataCacheInterp(metadata)
    ObjectService(permitsRef, objectServiceConfig, defaultGoogleStorageAlg, storageLinkAlg, metadataCacheAlg)
  }

  private def initObjectServiceWithMetadataCacheAndGoogleStorageAlg(
      storageLinks: Map[RelativePath, StorageLink],
      metadata: Ref[IO, Map[RelativePath, AdaptedGcsMetadataCache]],
      googleStorageAlg: GoogleStorageAlg
  ): ObjectService = {
    val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
    val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](storageLinks)
    val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
    val metadataCacheAlg = new MetadataCacheInterp(metadata)
    ObjectService(permitsRef, objectServiceConfig, googleStorageAlg, storageLinkAlg, metadataCacheAlg)
  }

  private def initObjectServiceWithGoogleStorageAlg(
      storageLinks: Map[RelativePath, StorageLink],
      metadata: Map[RelativePath, AdaptedGcsMetadataCache],
      googleStorageAlg: GoogleStorageAlg
  ): ObjectService = {
    val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](Map.empty[RelativePath, Semaphore[IO]])
    val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](storageLinks)
    val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](metadata)
    val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
    val metadataCacheAlg = new MetadataCacheInterp(metaCache)
    ObjectService(permitsRef, objectServiceConfig, googleStorageAlg, storageLinkAlg, metadataCacheAlg)
  }

  private def initObjectServiceWithPermits(permits: Map[RelativePath, Semaphore[IO]]): ObjectService = {
    val permitsRef = Ref.unsafe[IO, Map[RelativePath, Semaphore[IO]]](permits)
    val storageLinksCache = Ref.unsafe[IO, Map[RelativePath, StorageLink]](Map.empty)
    val metaCache = Ref.unsafe[IO, Map[RelativePath, AdaptedGcsMetadataCache]](Map.empty)
    val defaultGoogleStorageAlg = GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), FakeGoogleStorageInterpreter)
    val storageLinkAlg = StorageLinksAlg.fromCache(storageLinksCache)
    val metadataCacheAlg = new MetadataCacheInterp(metaCache)
    ObjectService(permitsRef, objectServiceConfig, defaultGoogleStorageAlg, storageLinkAlg, metadataCacheAlg)
  }
}

class MockGoogleStorageAlg extends GoogleStorageAlg {
  override def updateMetadata(gsPath: GsPath, traceId: TraceId, metadata: Map[String, String]): IO[UpdateMetadataResponse] = ???
  override def retrieveAdaptedGcsMetadata(localPath: RelativePath, gsPath: GsPath, traceId: TraceId): IO[Option[AdaptedGcsMetadata]] = ???
  override def removeObject(gsPath: GsPath, traceId: TraceId, generation: Option[Long]): Stream[IO, RemoveObjectResult] = ???
  override def gcsToLocalFile(localAbsolutePath: Path, gsPath: GsPath, traceId: TraceId): Stream[IO, AdaptedGcsMetadata] = ???
  override def delocalize(
      localObjectPath: RelativePath,
      gsPath: GsPath,
      generation: Long,
      userDefinedMeta: Map[String, String],
      traceId: TraceId
  ): IO[DelocalizeResponse] = ???
  override def localizeCloudDirectory(
      localBaseDirectory: RelativePath,
      cloudStorageDirectory: CloudStorageDirectory,
      workingDir: Path,
      pattern: Regex,
      traceId: TraceId
  ): Stream[IO, AdaptedGcsMetadataCache] = ???
  override def fileToGcs(localObjectPath: RelativePath, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = ???
  override def fileToGcsAbsolutePath(localFile: Path, gsPath: GsPath)(implicit ev: Ask[IO, TraceId]): IO[Unit] = ???
  def uploadBlob(bucketName: GcsBucketName, objectName: GcsBlobName): Pipe[IO, Byte, Unit] = ???
  def getBlob[A: Decoder](bucketName: GcsBucketName, blobName: GcsBlobName): Stream[IO, A] =
    for {
      blob <- FakeGoogleStorageInterpreter.getBlob(bucketName, blobName, None)
      a <- Stream
        .emits(blob.getContent())
        .through(fs2.text.utf8.decode)
        .through(_root_.io.circe.fs2.stringParser[IO](AsyncParser.SingleValue))
        .through(_root_.io.circe.fs2.decoder[IO, A])
    } yield a
}
