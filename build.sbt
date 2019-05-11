coverageMinimum := 15 //Update this once there're more tests
coverageFailOnMinimum := true

lazy val welder = project
  .in(file("."))
  .settings(
    skip in publish := true,
    Settings.commonSettings
  )
  .aggregate(core, server)

lazy val core =
  project
    .in(file("core"))
    .enablePlugins(BuildInfoPlugin)
    .settings(
      libraryDependencies ++= Dependencies.common,
      Settings.commonSettings,
      Settings.buildInfoSettings,
//    This is not ideal, but BuildInfoPlugin doesn't work as expected for core
      sourceGenerators in Compile += Def.task {
        val outDir = (sourceManaged in Compile).value / "welderBuildInfo"
        val outFile = new File(outDir, "buildinfo.scala")
        outDir.mkdirs
        val v = version.value
        val t = System.currentTimeMillis
        IO.write(
          outFile,
          s"""|package org.broadinstitute.dsp.workbench.welder
              |
            |/** Auto-generated build information. */
              |object BuildInfo {
              |  val version = "$v"
              |  val buildTime    = new java.util.Date(${t}L)
              |  val gitHeadCommit = "${git.gitHeadCommit.value.getOrElse("")}"
              |}
              |""".stripMargin
        )
        Seq(outFile)
      }.taskValue
    )

lazy val server =
  project
    .in(file("server"))
    .enablePlugins(JavaAppPackaging)
    .settings(
      libraryDependencies ++= Dependencies.server,
      Settings.serverSettings
    )
    .dependsOn(core % "test->test;compile->compile")
