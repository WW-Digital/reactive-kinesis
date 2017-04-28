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
        library.scalaTest % Test
      )
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {

    object Version {
      val scalaCheck = "1.13.5"
      val scalaTest = "3.0.3"
    }

    val scalaCheck = "org.scalacheck" %% "scalacheck" % Version.scalaCheck
    val scalaTest = "org.scalatest" %% "scalatest" % Version.scalaTest
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
  commonSettings ++
    headerSettings ++
    gitSettings

lazy val commonSettings =
  Seq(
    //version := "0.1.8", //TODO remove this - it will be calculated automatically by dynver
    //isSnapshot := false, //TODO remove this - it will be calculated automatically by dynver
    // scalaVersion from .travis.yml via sbt-travisci //TODO
    organization := "com.weightwatchers",
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

/* This allows to derive an sbt version string from the git information.
 * The logic goes as follows :
 *
 * IF the current commit is tagged with "vX.Y.Z" (ie semantic-versioning), the version is "X.Y.Z"
 * ELSE IF the current commit is tagged with "vX.Y.Z-SNAPSHOT", the version is "X.Y.Z-commitsSinceVersion-SNAPSHOT"
 * ELSE IF the latest found tag is "vX.Y.Z", the version is "X.Y.Z-commitsSinceVersion-gCommitHash-SNAPSHOT"
 * ELSE the version is "0.0.0-commitHash-SNAPSHOT"
 */
val VersionRegex = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r
lazy val gitSettings =
  Seq(
    git.baseVersion := "0.0.0",
    git.useGitDescribe := true,
    git.gitTagToVersionNumber := {
      case VersionRegex(v, "")         => Some("222"+v)
      case VersionRegex(v, "SNAPSHOT") => Some(s"$v-SNAPSHOT2")
      case VersionRegex(v, s)          => Some(s"$v-$s-SNAPSHOT3")
      case _                           => Some("serewre")
    }
  )

import de.heikoseeberger.sbtheader.HeaderPattern
import de.heikoseeberger.sbtheader.license._

lazy val headerSettings =
  Seq(
    headers := Map("scala" -> Apache2_0("2017", "WeightWatchers"))
  )

