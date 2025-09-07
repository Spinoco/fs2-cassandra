import sbt.Tests.{Group, SubProcess}
import xerial.sbt.Sonatype.sonatypeCentralHost

val ReleaseTag = """^release/([\d\.]+a?)$""".r

lazy val contributors = Seq(
  "pchlupacek" -> "Pavel Chlupáček"
  , "adamchlupacek" -> "Adam Chlupáček"
  , "kareltucek" -> "Karel Tuček"
  , "mraulim" -> "Milan Raulim"
)

/**
 * Profiling notes:
 * - Add these options to commonSettings.scalaOptions:
 *   ```
 *   , "-Ystatistics:typer"
 *   , "-P:scalac-profiling:generate-global-flamegraph"
 *   , "-P:scalac-profiling:generate-macro-flamegraph"
 *   , "-P:scalac-profiling:show-concrete-implicit-tparams"
 *   , "-P:scalac-profiling:print-failed-implicit-macro-candidates"
 *   , "-P:scalac-profiling:show-profiles"
 *   , "-P:scalac-profiling:print-search-results"
 *   , "-Xprint:all"
 *   ```
 * - Add this to commonSettings:
 *   ```
 *   , addCompilerPlugin("ch.epfl.scala" %% "scalac-profiling" % "1.1.2" cross CrossVersion.full)
 *   ```
 */

lazy val commonSettings = Seq(
  organization := "com.spinoco",
  scalaVersion := "2.13.16",
  crossScalaVersions := Seq("2.13.16", "2.12.20"),
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:existentials",
    "-language:postfixOps",
    "-Xfatal-warnings"
  ) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 12)) => Seq(
      "-Yno-adapted-args",
      "-Ywarn-value-discard",
      "-Ywarn-unused-import"
    )
    case Some((2, 13)) => Seq(
      "-Wvalue-discard",
      "-Wunused:imports"
    )
    case _ => Seq.empty
  }),
  scmInfo := Some(ScmInfo(url("https://github.com/Spinoco/fs2-cassandra"), "git@github.com:Spinoco/fs2-cassandra.git")),
  homepage := None,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  initialCommands := s"""
    import fs2._
    import spinoco.fs2.cassandra._
  """
  , libraryDependencies ++= Seq(
    "co.fs2" %% "fs2-core" % "3.12.2"
    , "co.fs2" %% "fs2-io" % "3.12.2"
    , "com.datastax.oss" % "java-driver-core" % "4.17.0"
    , "com.chuusai" %% "shapeless" % "2.3.13"
    , "org.scodec" %% "scodec-core" % "1.11.11"
    , "org.scala-lang" % "scala-reflect" % scalaVersion.value
    , "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0"
  )
  , libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 12)) => Seq(compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full))
    case _ => Seq.empty
  })
) ++ testSettings ++ publishingSettings ++ releaseSettings

lazy val testSettings = Seq(
  parallelExecution := false,
  fork := true,
  testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")
)


lazy val publishingSettings = Seq(
  sonatypeCredentialHost := sonatypeCentralHost,
  publishTo := sonatypePublishToBundle.value,
  versionScheme := Some("early-semver"),
  organization := "com.spinoco",
  homepage := Some(url("https://github.com/spinoco/fs2-cassandra")),
  licenses := List("MIT" -> url("http://opensource.org/licenses/MIT")),
  developers := {
    for ((username, name) <- contributors) yield
      Developer(
        username,
        name,
        "",
        url(s"https://github.com/$username")
      )
  }.toList,
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/spinoco/fs2-cassandra"),
      "scm:git@github.com:spinoco/fs2-cassandra.git"
    )
  )
)

lazy val releaseSettings = Seq(
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value
)

lazy val noPublish = Seq(
  publish := (()),
  publishLocal := (()),
  publishArtifact := false
)

lazy val macros =
  project.in(file("macros"))
    .settings(commonSettings)
    .settings(
      name := "fs2-cassandra-macros"
    )

lazy val core =
  project.in(file("core"))
  .settings(commonSettings)
  .settings(
   name := "fs2-cassandra"
  )
  .dependsOn(macros)

lazy val testSupport =
  project.in(file("test-support"))
  .settings(commonSettings)
  .settings(
    name := "fs2-cassandra-test-support"
    , libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.18"
      , "org.scalacheck" %% "scalacheck" % "1.17.0"
      , "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0"
      //, "org.slf4j" % "slf4j-simple" % "1.6.1"  // uncomment this for logs when testing
    )
  )
  .dependsOn(core, macros)

lazy val coreTest =
  project.in(file("test"))
  .settings(commonSettings ++ noPublish)
  .settings(
    name := "fs2-cassandra-test"
  )
  .dependsOn(
    core
    , testSupport % "test"
    , macros
  )

lazy val fs2Cassandra =
  project.in(file("."))
  .settings(commonSettings ++ noPublish)
  .aggregate(
    core, testSupport, coreTest, macros
  )

lazy val doNotPublish = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  //skip in publish := true
)

