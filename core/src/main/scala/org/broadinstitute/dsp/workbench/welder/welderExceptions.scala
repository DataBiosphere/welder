package org.broadinstitute.dsp.workbench.welder

import org.broadinstitute.dsde.workbench.model.TraceId

import scala.util.control.NoStackTrace

sealed abstract class WelderException extends NoStackTrace {
  def message: String
  def traceId: TraceId
  override def getMessage: String = s"${traceId} | $message"
}
final case class InternalException(traceId: TraceId, message: String) extends WelderException
final case class BadRequestException(traceId: TraceId, message: String) extends WelderException
final case class GenerationMismatch(traceId: TraceId, message: String) extends WelderException
final case class StorageLinkNotFoundException(traceId: TraceId, message: String) extends WelderException
final case class SafeDelocalizeSafeModeFileError(traceId: TraceId, message: String) extends WelderException
final case class InvalidLock(traceId: TraceId, message: String) extends WelderException
final case class DeleteSafeModeFileError(traceId: TraceId, message: String) extends WelderException
final case class NotFoundException(traceId: TraceId, message: String) extends WelderException
final case class LockedByOther(traceId: TraceId, message: String) extends WelderException
