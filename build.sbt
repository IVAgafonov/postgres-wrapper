ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "postgres-wrapper"
  )

ThisBuild / organization := "io.github.ivagafonov"
ThisBuild / versionScheme := Some("early-semver")

ThisBuild / homepage := Some(url("https://github.com/IVAgafonov/metrics-support"))
ThisBuild / licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    "Igor <igoradm90@gmail.com>",
    "Igor Agafonov",
    "igoradm90@gmail.com",
    url("https://github.com/IVAgafonov"))
)


libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-dbcp2" % "2.14.0",
  "org.postgresql" % "postgresql" % "42.7.3"
)
