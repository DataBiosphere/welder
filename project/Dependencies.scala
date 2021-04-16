import sbt._

object Dependencies {
  val circeVersion = "0.13.0"
  val http4sVersion = "0.21.22"
  val grpcCoreVersion = "1.34.0"
  val scalaTestVersion = "3.2.7"
  val workbenchGoogle2V = "0.21-197294d"

  val common = List(
    "com.github.pureconfig" %% "pureconfig" % "0.14.1",
    "co.fs2" %% "fs2-io" % "2.5.3",
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "org.http4s" %% "http4s-blaze-client" % http4sVersion,
    "org.http4s" %% "http4s-circe" % http4sVersion,
    "org.http4s" %% "http4s-dsl" % http4sVersion,
    "org.log4s" %% "log4s" % "1.8.2",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.scalatestplus" %% "scalacheck-1-14" % "3.2.0.0" % Test,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V % "test" classifier "tests", //for generators
    "ca.mrvisser" %% "sealerate" % "0.0.6",
    "com.google.cloud" % "google-cloud-nio" % "0.122.11" % "test",
    "net.logstash.logback" % "logstash-logback-encoder" % "6.2" // for structured logging in logback
  )

  val server = common ++ List(
    "org.http4s" %% "http4s-blaze-server" % http4sVersion,
    "io.grpc" % "grpc-core" % grpcCoreVersion
  )
}
