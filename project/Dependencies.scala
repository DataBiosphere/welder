import sbt._

object Dependencies {
  val circeVersion = "0.13.0"
  val http4sVersion = "0.21.7"
  val grpcCoreVersion = "1.28.0"
  val scalaTestVersion = "3.2.2"
  val workbenchGoogle2V = "0.13-39c1b35"

  val common = List(
    "com.github.pureconfig" %% "pureconfig" % "0.14.0",
    "co.fs2" %% "fs2-io" % "2.4.2",
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "io.sentry" % "sentry-logback" % "1.7.16", // see doc https://docs.sentry.io/clients/java/modules/logback/
    "org.http4s" %% "http4s-blaze-client" % http4sVersion,
    "org.http4s" %% "http4s-circe" % http4sVersion,
    "org.http4s" %% "http4s-dsl" % http4sVersion,
    "org.log4s" %% "log4s" % "1.8.2",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.scalatestplus" %% "scalacheck-1-14" % "3.2.0.0" % Test,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V % "test" classifier "tests", //for generators
    "ca.mrvisser" %% "sealerate" % "0.0.6",
    "com.google.cloud" % "google-cloud-nio" % "0.107.0-alpha" % "test",
    "net.logstash.logback" % "logstash-logback-encoder" % "6.2" // for structured logging in logback
  )

  val server = common ++ List(
    "org.http4s" %% "http4s-blaze-server" % http4sVersion,
    "io.grpc" % "grpc-core" % grpcCoreVersion
  )
}
