import sbt.Tests.{Group, SubProcess}

val ReleaseTag = """^release/([\d\.]+a?)$""".r

lazy val contributors = Seq(
  "pchlupacek" -> "Pavel Chlupáček"
  , "adamchlupacek" -> "Adam Chlupáček"
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
  scalaVersion := "2.12.20",
  crossScalaVersions := Seq("2.12.20"),
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:existentials",
    "-language:postfixOps",
    "-Xfatal-warnings",
    "-Yno-adapted-args",
    "-Ywarn-value-discard",
    "-Ywarn-unused-import"
  ),
  scalacOptions --= Seq("-Ywarn-unused-import", "-Ywarn-unused:imports"),
  scmInfo := Some(ScmInfo(url("https://github.com/Spinoco/fs2-cassandra"), "git@github.com:Spinoco/fs2-cassandra.git")),
  homepage := None,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  initialCommands := s"""
    import fs2._
    import fs2.util._
    import spinoco.fs2.cassandra._
  """
  , libraryDependencies ++= Seq(
    "co.fs2" %% "fs2-core" % "1.0.0"
    , "co.fs2" %% "fs2-io" % "1.0.0"
    , "com.datastax.oss" % "java-driver-core" % "4.17.0"
    , "com.chuusai" %% "shapeless" % "2.3.13"
    , "org.scodec" %% "scodec-core" % "1.10.3"
  )
  , addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
) ++ testSettings //++ scaladocSettings ++ publishingSettings ++ releaseSettings

lazy val testSettings = Seq(
  parallelExecution := false,
  fork := true,
  testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  Test / testGrouping := (Test / definedTests).map { tests =>
    // group tests individually to fork them in JVM.
    // essentially any CassandraIntegration_* id having its own group, all others share a group
    // this is necessary hence JavaDriver seems to share some sort of global state preventing to switch
    // different cluster versions correctly in single JVM
    tests.groupBy { td =>
      if (td.name.contains(".CassandraIntegration")) {
        td.name
      } else "default_group"
    }.map { case (groupName, tests) =>
      Group(
        name = groupName
        , tests = tests
        , runPolicy = Tests.SubProcess(ForkOptions())
      )
    }.toSeq
  }.value
)

lazy val scaladocSettings = Seq(
)

lazy val publishingSettings = Seq(
  publishArtifact  := false
  , publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  credentials ++= Seq(Credentials(Path.userHome / ".ivy2" / ".credentials.sonatype")) ++ (for {
    username <- Option(System.getenv().get("SONATYPE_USERNAME"))
    password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/Spinoco/fs2-cassandra.git</url>
    <developers>
      {for ((username, name) <- contributors) yield
      <developer>
        <id>{username}</id>
        <name>{name}</name>
        <url>http://github.com/{username}</url>
      </developer>
      }
    </developers>
  }
  ,pomPostProcess := { node =>
    import scala.xml._
    import scala.xml.transform._
    def stripIf(f: Node => Boolean) = new RewriteRule {
      override def transform(n: Node) =
        if (f(n)) NodeSeq.Empty else n
    }
    val stripTestScope = stripIf { n => n.label == "dependency" && (n \ "scope").text == "test" }
    new RuleTransformer(stripTestScope).transform(node)(0)
  }
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
      "org.scalatest" %% "scalatest" % "3.0.4"
      , "org.scalacheck" %% "scalacheck" % "1.13.4"
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

// CI build
addCommandAlias("ciBuild", ";clean;project coreTest;test;project microsite;tut")
