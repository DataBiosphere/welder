package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import fs2.Stream
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse}
import org.broadinstitute.dsde.workbench.google2.mock.BaseFakeGoogleStorage
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

class FakeGoogleStorageService(metadataResponse: GetMetadataResponse) extends BaseFakeGoogleStorage {
  override def getObjectMetadata(
      bucketName: GcsBucketName,
      blobName: GcsBlobName,
      traceId: Option[TraceId],
      retryConfig: RetryConfig
  ): fs2.Stream[IO, GetMetadataResponse] = Stream.emit(metadataResponse).covary[IO]
}

object FakeGoogleStorageService {
  def apply(metadata: GetMetadataResponse): FakeGoogleStorageService = new FakeGoogleStorageService(metadata)
}
