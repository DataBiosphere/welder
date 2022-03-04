package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.effect.Ref
import cats.effect.std.Semaphore

package object server {
  type Permits = Ref[IO, Map[RelativePath, Semaphore[IO]]]
}
