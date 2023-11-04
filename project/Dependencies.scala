import sbt._

object Dependencies {
  val circeVersion = "0.14.3"
  val http4sVersion = "1.0.0-M35"
  val grpcCoreVersion = "1.51.0"
  val scalaTestVersion = "3.2.14"

  val workbenchLibsHash = "1a6839f"
  val workbenchGoogle2V = s"0.25-$workbenchLibsHash"
  val workbenchAzureV = s"0.1-$workbenchLibsHash"

  val common = List(
    "com.github.pureconfig" %% "pureconfig" % "0.17.2",
    "co.fs2" %% "fs2-io" % "3.4.0",
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "org.http4s" %% "http4s-blaze-client" % http4sVersion,
    "org.http4s" %% "http4s-circe" % http4sVersion,
    "org.http4s" %% "http4s-dsl" % http4sVersion,
    "org.log4s" %% "log4s" % "1.10.0",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.scalatestplus" %% "scalacheck-1-15" % "3.2.11.0" % Test,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V,
    "org.broadinstitute.dsde.workbench" %% "workbench-google2" % workbenchGoogle2V % "test" classifier "tests", //for generators
    "org.broadinstitute.dsde.workbench" %% "workbench-azure" % workbenchAzureV,
    "org.broadinstitute.dsde.workbench" %% "workbench-azure" % workbenchAzureV % "test" classifier "tests", //for generators/mocks
    "ca.mrvisser" %% "sealerate" % "0.0.6",
    "com.google.cloud" % "google-cloud-nio" % "0.127.7" % "test",
    "ch.qos.logback" % "logback-classic" % "1.4.5",
    "net.logstash.logback" % "logstash-logback-encoder" % "7.2" // for structured logging in logback
  )

  val server = common ++ List(
    "org.http4s" %% "http4s-blaze-server" % http4sVersion,
    "io.grpc" % "grpc-core" % grpcCoreVersion
  )
}
