import sbt._

object Dependencies {
  val circeVersion = "0.10.0"
  val http4sVersion = "0.20.0"
  val grpcCoreVersion = "1.17.1"
  val scalaTestVersion = "3.0.7"
  val newRelicVersion = "5.0.0"

  val common = List(
    "com.github.pureconfig" %% "pureconfig" % "0.11.0",
    "io.chrisdavenport" %% "log4cats-slf4j" % "0.3.0",
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "io.sentry" % "sentry-logback" % "1.7.16", // see doc https://docs.sentry.io/clients/java/modules/logback/
    "org.http4s" %% "http4s-blaze-client" % http4sVersion,
    "org.http4s" %% "http4s-circe" % http4sVersion,
    "org.http4s" %% "http4s-dsl" % http4sVersion,
    "org.log4s" %% "log4s" % "1.7.0",
    "org.webjars" % "swagger-ui" % "3.20.1",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % "0.2-4c7acd5",
    "com.newrelic.agent.java" % "newrelic-api" % newRelicVersion
  )

  val server = common ++ List(
    "org.http4s" %% "http4s-blaze-server" % http4sVersion,
    "io.grpc" % "grpc-core" % grpcCoreVersion
  )
}
