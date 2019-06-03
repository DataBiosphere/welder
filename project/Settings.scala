import java.time.ZoneId

import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.ExecCmd
import org.scalafmt.sbt.ScalafmtPlugin.autoImport.scalafmtOnCompile
import sbt.Keys.{scalacOptions, _}
import sbt._
import com.typesafe.sbt.GitPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import com.gilt.sbt.newrelic.NewRelic.autoImport._

object Settings {
  lazy val artifactory = "https://artifactory.broadinstitute.org/artifactory/"

  lazy val commonResolvers = List(
    "artifactory-releases" at artifactory + "libs-release",
    "artifactory-snapshots" at artifactory + "libs-snapshot"
  )

  //coreDefaultSettings + defaultConfigs = the now deprecated defaultSettings
  lazy val commonBuildSettings = Defaults.coreDefaultSettings ++ Defaults.defaultConfigs ++ Seq(
    javaOptions += "-Xmx2G",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings")
  )

  lazy val buildInfoSettings = List(
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      version,
      scalaVersion,
      sbtVersion,
      git.gitHeadCommit,
      BuildInfoKey.action("buildTime") {
        java.time.LocalDateTime.now(ZoneId.systemDefault()).toString
      }
    )
  )

  // recommended scalac options by https://tpolecat.github.io/2017/04/25/scalac-flags.html
  lazy val commonCompilerSettings = Seq(
    "-target:jvm-1.8",
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-encoding",
    "utf-8", // Specify character encoding used by source files.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    "-Xfatal-warnings", // Fail the compilation if there are any warnings.
    "-Xfuture", // Turn on future language features.
    "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
    "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
    "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
    "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Xlint:option-implicit", // Option.apply used implicit view.
    "-Xlint:package-object-classes", // Class or object defined in package object.
    "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
    "-Xlint:unsound-match", // Pattern match may not be typesafe.
    "-Ypartial-unification", // Enable partial unification in type constructor inference
    "-Ywarn-dead-code", // Warn when dead code is identified.
    "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
    "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Ywarn-numeric-widen", // Warn when numerics are widened.
    "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-language:postfixOps",
    "-language:higherKinds",
    "-Ywarn-value-discard" // Warn when non-Unit expression results are unused.
  )

  //common settings for all sbt subprojects
  lazy val commonSettings =
    commonBuildSettings ++ List(
      organization := "org.broadinstitute.dsp.workbench",
      scalaVersion := "2.12.8",
      resolvers ++= commonResolvers,
      scalacOptions ++= commonCompilerSettings,
      scalafmtOnCompile := true,
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.9"),
      addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.0")
    )

  lazy val commonDockerSettings = List(
    maintainer := "workbench-interactive-analysis@broadinstitute.org",
    dockerBaseImage := "oracle/graalvm-ce:1.0.0-rc16",
    dockerRepository := Some("us.gcr.io"),
    dockerExposedPorts := List(8080),
    dockerUpdateLatest := true,
    dockerCommands ++= List(
      ExecCmd("CMD", "export", "CLASSPATH=lib/*jar")
    )
  )

  lazy val serverDockerSettings = commonDockerSettings ++ List(
    mainClass in Compile := Some("org.broadinstitute.dsp.workbench.welder.server.Main"),
    packageName in Docker := "broad-dsp-gcr-public/welder-server",
    dockerEntrypoint := List("/opt/docker/bin/server", "-Dconfig.file=/etc/welder.conf"), //TODO: put /etc/welder.conf on the image
    dockerAlias := DockerAlias(
      Some("us.gcr.io"),
      None,
      "broad-dsp-gcr-public/welder-server",
      git.gitHeadCommit.value.map(_.substring(0, 7))
    )
  )

  lazy val serverSettings = commonSettings ++ serverDockerSettings
}
