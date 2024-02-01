import java.time.ZoneId

import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}
import org.scalafmt.sbt.ScalafmtPlugin.autoImport.scalafmtOnCompile
import sbt.Keys.{scalacOptions, _}
import sbt._
import com.github.sbt.git.GitPlugin.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._

object Settings {
  lazy val artifactory = "https://broadinstitute.jfrog.io/artifactory/"

  lazy val commonResolvers = List(
    "artifactory-releases" at artifactory + "libs-release",
    "artifactory-snapshots" at artifactory + "libs-snapshot"
  )

  //coreDefaultSettings + defaultConfigs = the now deprecated defaultSettings
  lazy val commonBuildSettings = Defaults.coreDefaultSettings ++ Defaults.defaultConfigs ++ Seq(
    javaOptions += "-Xmx2G",
    javacOptions ++= Seq("--release", "17"),
    run / fork := true,
    Compile / console / scalacOptions --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings")
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
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-encoding",
    "utf-8", // Specify character encoding used by source files.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
    "-Xfatal-warnings", // Fail the compilation if there are any warnings.
    "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
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
      scalaVersion := "2.13.12",
      resolvers ++= commonResolvers,
      scalacOptions ++= commonCompilerSettings,
      scalafmtOnCompile := true,
      addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
    )

  lazy val entrypoint = "/opt/docker/bin/entrypoint.sh"
  lazy val entrypointLog = "/work/.entrypoint.out"

  lazy val entrypointCmd =
    s"""'#!/bin/bash\\numask 002;\\necho '[$$(date -u)] Starting welder java process' >> $entrypointLog\\nuntil /opt/docker/bin/server; do\\n\\techo '[$$(date -u)] Welder crashed. Respawning..' >> $entrypointLog\\n\\tsleep 1\\ndone'""".stripMargin

  lazy val commonDockerSettings = List(
    maintainer := "workbench-interactive-analysis@broadinstitute.org",
    dockerBaseImage := "us.gcr.io/broad-dsp-gcr-public/base/jre:17-debian",
    dockerRepository := Some("us.gcr.io"),
    dockerExposedPorts := List(8080),
    dockerEnvVars := Map("JAVA_OPTS" -> "-server -Xmx512m -Xms512m"),
    dockerUpdateLatest := true,
    // See https://www.scala-sbt.org/sbt-native-packager/formats/docker.html#add-commands
    dockerCommands ++= List(
      // Change the default umask for welder to support R/W access to the shared volume
      Cmd("USER", "root"),
      ExecCmd("RUN", "/bin/bash", "-c", s"echo $entrypointCmd > $entrypoint"),
      Cmd("RUN", s"chown -R welder-user:users /opt/docker && chmod u+x,g+x $entrypoint"),
      Cmd("USER", (Docker / daemonUser).value)
    )
  )

  lazy val serverDockerSettings = commonDockerSettings ++ List(
    Compile / mainClass := Some("org.broadinstitute.dsp.workbench.welder.server.Main"),
    Docker / packageName := "broad-dsp-gcr-public/welder-server",
    // user, uid, group, and gid are all replicated in the Jupyter container
    Docker / daemonUser := "welder-user",
    Docker / daemonUserUid := Some("1001"),
    Docker / daemonGroup := "users",
    Docker / daemonGroupGid := Some("100"),
    dockerEntrypoint := List(entrypoint),
    dockerAliases := List(
      DockerAlias(
        Some("us.gcr.io"),
        None,
        "broad-dsp-gcr-public/welder-server",
        git.gitHeadCommit.value.map(_.substring(0, 7))
      ),
      DockerAlias(
        Some("terradevacrpublic.azurecr.io"),
        None,
        "welder-server",
        git.gitHeadCommit.value.map(_.substring(0, 7))
      )
    )
  )

  lazy val serverSettings = commonSettings ++ serverDockerSettings
}
