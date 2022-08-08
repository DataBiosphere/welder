#Testing `GoogleStorageInterp` Locally

This is an example on how to test Google2 methods locally. In this example, we are testing the 
GoogleStorageInterpreter.getObjectMetadata method. Before this, we have downloaded a credential JSON 
file with which we can create a GoogleStorageService object.

Run `sbt server/console`

```scala
// copy+paste to import all these

import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import scala.concurrent.ExecutionContext.global
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.effect.IO
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.typelevel.linebacker.Linebacker

implicit val cs = IO.contextShift(global)
implicit val t = IO.timer(global)
implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
implicit val lineBacker = Linebacker.fromExecutionContext[IO](global)

import org.broadinstitute.dsp.workbench.welder.CloudStorageAlg
import org.broadinstitute.dsp.workbench.welder.GoogleStorageAlgConfig
import java.nio.file.Paths
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.LocalBaseDirectory
import org.broadinstitute.dsp.workbench.welder.CloudStorageDirectory
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsp.workbench.welder.RelativePath
import org.broadinstitute.dsp.workbench.welder.LocalDirectory.LocalBaseDirectory
import java.util.UUID

val traceId = TraceId(UUID.randomUUID)
```

`scala> GoogleStorageService.resource[IO]("credentials.json")`

SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
res0: cats.effect.Resource[cats.effect.IO,org.broadinstitute.dsde.workbench.google2.GoogleStorageService[cats.effect.IO]] = Bind(Bind(Allocate(<function1>),org.broadinstitute.dsde.workbench.google2.GoogleStorageInterpreter$$$Lambda$5201/1070179104@19a35b53),cats.effect.Resource$$Lambda$5203/746114085@43398b6)

`scala> val googleStorageAlg = res0.map(s => GoogleStorageAlg.fromGoogle(GoogleStorageAlgConfig(Paths.get("/tmp")), s))`

res2: cats.effect.IO[Map[String,String]] = IO$1818107578

`scala> val res = googleStorageAlg.use(s => s.localizeCloudDirectory(LocalBaseDirectory(RelativePath(Paths.get("edit"))), CloudStorageDirectory(GcsBucketName("qi-test"),None), Paths.get("/tmp"), traceId).compile.drain)`
`scala> res.unsafeRunSync()`