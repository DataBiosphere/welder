import sbt._

object Dependencies {
  val circeVersion = "0.13.0"
  val http4sVersion = "0.21.1"
  val grpcCoreVersion = "1.17.1"
  val scalaTestVersion = "3.1.1"
  val workbenchGoogle2V = "0.11-8d09185"

  val common = List(
    "com.github.pureconfig" %% "pureconfig" % "0.12.1",
    "co.fs2" %% "fs2-io" % "2.2.2",
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "io.sentry" % "sentry-logback" % "1.7.16", // see doc https://docs.sentry.io/clients/java/modules/logback/
    "org.http4s" %% "http4s-blaze-client" % http4sVersion,
    "org.http4s" %% "http4s-circe" % http4sVersion,
    "org.http4s" %% "http4s-dsl" % http4sVersion,
    "org.log4s" %% "log4s" % "1.7.0",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2" % Test,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V % "test" classifier "tests", //for generators
    "ca.mrvisser" %% "sealerate" % "0.0.6",
    "com.google.cloud" % "google-cloud-nio" % "0.101.0-alpha" % "test",
    "net.logstash.logback" % "logstash-logback-encoder" % "6.2" // for structured logging in logback
  )

  val server = common ++ List(
    "org.http4s" %% "http4s-blaze-server" % http4sVersion,
    "io.grpc" % "grpc-core" % grpcCoreVersion
  )
}
