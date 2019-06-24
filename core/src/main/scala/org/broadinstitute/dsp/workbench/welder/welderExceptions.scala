package org.broadinstitute.dsp.workbench.welder

import scala.util.control.NoStackTrace

sealed abstract class WelderException extends NoStackTrace {
  def message: String
  override def getMessage: String = message
}
final case class InternalException(message: String) extends WelderException
final case class BadRequestException(message: String) extends WelderException
final case class GenerationMismatch(message: String) extends WelderException
final case class StorageLinkNotFoundException(message: String) extends WelderException
final case class SafeDelocalizeSafeModeFileError(message: String) extends WelderException
final case class DeleteSafeModeFileError(message: String) extends WelderException
final case class UnknownFileState(message: String) extends WelderException
final case class NotFoundException(message: String) extends WelderException
