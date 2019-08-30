package org.broadinstitute.dsp.workbench.welder

import cats.effect.IO
import cats.effect.concurrent.{Ref, Semaphore}

package object server {
  type Permits = Ref[IO, Map[RelativePath, Semaphore[IO]]]
}
