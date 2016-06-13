import com.typesafe.sbt.pgp.PgpKeys.publishSigned
import sbt.Tests.{Group, SubProcess}

val ReleaseTag = """^release/([\d\.]+a?)$""".r

lazy val contributors = Seq(
  "pchlupacek" -> "Pavel Chlupáček"
)

lazy val commonSettings = Seq(
  organization := "com.spinoco",
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq("2.11.8"),
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
  scalacOptions in (Compile, console) ~= {_.filterNot("-Ywarn-unused-import" == _)},
  scalacOptions in (Test, console) <<= (scalacOptions in (Compile, console)),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.0-M16-SNAP4" % "test"
    , "org.scalacheck" %% "scalacheck" % "1.13.1" % "test"
    , "org.apache.cassandra" % "cassandra-all" % "2.1.10" % "test"
    //, "org.slf4j" % "slf4j-simple" % "1.6.1" % "test" // uncomment this for logs when testing

    , "co.fs2" %% "fs2-core" % "0.9.0-SNAPSHOT"
    , "co.fs2" %% "fs2-io" % "0.9.0-SNAPSHOT"
    , "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.1"
    , "com.chuusai" %% "shapeless" % "2.3.1"

    // as per https://github.com/google/guava/issues/1095
    , "com.google.code.findbugs" % "jsr305" % "1.3.+" % "compile"

  ),
  scmInfo := Some(ScmInfo(url("https://github.com/Spinoco/fs2-cassabdra"), "git@github.com:Spinoco/fs2-spinoco.fs2.cassandra.git")),
  homepage := None,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  initialCommands := s"""
    import fs2._
    import fs2.util._
    import spinoco.fs2.cassandra._
  """
) ++ testSettings ++ scaladocSettings ++ publishingSettings ++ releaseSettings

lazy val testSettings = Seq(
  parallelExecution in Test := false,
  fork in Test := true,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  publishArtifact in Test := true,
  testGrouping in Test <<= definedTests in Test map { tests =>
    // group tests indivduially to fork them in JVM.
    // essentially any CassandraIntegration_* id having its own group, all others share a group
    // this is necessary hence JavaDriver seems to share some sort of global state preventing to switch
    // different cluster versions correctly in single JVM
    tests.groupBy { td =>
      if (td.name.contains(".CassandraIntegration")) {
        td.name
      } else "default_group"
    }.map { case (groupName, tests) =>
      new Group(
        name = groupName
        , tests = tests
        , runPolicy = Tests.SubProcess(ForkOptions())
      )
    }.toSeq
  }
)

lazy val scaladocSettings = Seq(
  scalacOptions in (Compile, doc) ++= Seq(
    "-doc-source-url", scmInfo.value.get.browseUrl + "/tree/master€{FILE_PATH}.scala",
    "-sourcepath", baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-implicits",
    "-implicits-show-all"
  ),
  scalacOptions in (Compile, doc) ~= { _ filterNot { _ == "-Xfatal-warnings" } },
  autoAPIMappings := true
)

lazy val publishingSettings = Seq(
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  credentials ++= (for {
    username <- Option(System.getenv().get("SONATYPE_USERNAME"))
    password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/Spinoco/fs2-zk</url>
    <scm>
      <url>git@github.com:functional-streams-for-scala/fs2.git</url>
      <connection>scm:git:git@github.com:Spinoco/fs2-zk.git</connection>
    </scm>
    <developers>
      {for ((username, name) <- contributors) yield
      <developer>
        <id>{username}</id>
        <name>{name}</name>
        <url>http://github.com/{username}</url>
      </developer>
      }
    </developers>
  },
  pomPostProcess := { node =>
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

lazy val root = 
  project.in(file("."))
  .settings(commonSettings)
  .settings(
   name := "fs2-spinoco.fs2.cassandra"
  ) 
 
 

