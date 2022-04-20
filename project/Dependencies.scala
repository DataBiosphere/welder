import sbt._

object Dependencies {
  val circeVersion = "0.14.1"
  val http4sVersion = "1.0.0-M32"
  val grpcCoreVersion = "1.34.0"
  val scalaTestVersion = "3.2.7"
  val workbenchGoogle2V = "0.24-89b188f"

  val common = List(
    "com.github.pureconfig" %% "pureconfig" % "0.14.1",
    "co.fs2" %% "fs2-io" % "3.2.4",
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "org.http4s" %% "http4s-blaze-client" % http4sVersion,
    "org.http4s" %% "http4s-circe" % http4sVersion,
    "org.http4s" %% "http4s-dsl" % http4sVersion,
    "org.log4s" %% "log4s" % "1.8.2",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.scalatestplus" %% "scalacheck-1-15" % "3.2.11.0" % Test,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V % "test" classifier "tests", //for generators
    "ca.mrvisser" %% "sealerate" % "0.0.6",
    "com.google.cloud" % "google-cloud-nio" % "0.123.28" % "test",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "net.logstash.logback" % "logstash-logback-encoder" % "6.2" // for structured logging in logback
  )

  val server = common ++ List(
    "org.http4s" %% "http4s-blaze-server" % http4sVersion,
    "io.grpc" % "grpc-core" % grpcCoreVersion
  )
}
