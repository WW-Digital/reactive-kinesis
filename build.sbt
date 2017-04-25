// *****************************************************************************
// Projects
// *****************************************************************************

lazy val `reactive-kinesis` =
  project
    .in(file("."))
    .enablePlugins(AutomateHeaderPlugin, GitVersioning)
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        library.scalaCheck % Test,
        library.scalaTest  % Test
      )
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val scalaCheck = "1.13.5"
      val scalaTest  = "3.0.3"
    }
    val scalaCheck = "org.scalacheck" %% "scalacheck" % Version.scalaCheck
    val scalaTest  = "org.scalatest"  %% "scalatest"  % Version.scalaTest
  }

// *****************************************************************************
// Settings
// *****************************************************************************


inThisBuild(publishSettings)

lazy val publishSettings = 
  Seq(
  // scalaVersion from .travis.yml via sbt-travisci
  organization := "com.weightwatchers",
  pomIncludeRepository := { _ => false }, //remove optional dependencies from our pom
  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage := Some(url("http://www.weightwatchers.com")),
  scmInfo := Some(ScmInfo(url("https://github.com/WW-Digital/reactive-kinesis"), "scm:git@github.com:WW-Digital/reactive-kinesis.git")),
  developers := List(Developer("markglh", "Mark Harrison", "markglh@gmail.com", url("https://github.com/markglh"))),
  publishArtifact in Test := false,
  bintrayReleaseOnPublish := false
)

lazy val settings =
  commonSettings ++
  headerSettings

lazy val commonSettings =
  Seq(
    mappings.in(Compile, packageBin) += baseDirectory.in(ThisBuild).value / "LICENSE" -> "LICENSE",
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding", "UTF-8"
    ),
    unmanagedSourceDirectories.in(Compile) := Seq(scalaSource.in(Compile).value),
    unmanagedSourceDirectories.in(Test) := Seq(scalaSource.in(Test).value),
    shellPrompt in ThisBuild := { state =>
      val project = Project.extract(state).currentRef.project
      s"[$project]> "
    }
)

import de.heikoseeberger.sbtheader.HeaderPattern
import de.heikoseeberger.sbtheader.license._
lazy val headerSettings =
  Seq(
    headers := Map("scala" -> Apache2_0("2017", "WeightWatchers"))
  )

