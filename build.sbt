// *****************************************************************************
// Projects
// *****************************************************************************

val core =
  project
    .in(file("modules/core"))
    .settings(name := "core")
    .enablePlugins(AutomateHeaderPlugin, GitVersioning, ScalafmtCorePlugin)
    .settings(settings)
    .settings(
      libraryDependencies ++=
        Dependencies.compile ++ Dependencies.test
    )

val consumer =
  project
    .in(file("modules/consumer"))
    .settings(name := "consumer")
    .dependsOn(core)
    .enablePlugins(AutomateHeaderPlugin, GitVersioning, ScalafmtCorePlugin)
    .settings(settings)
    .settings(
      libraryDependencies ++=
        Dependencies.compile ++ Dependencies.test
    )

val producer =
  project
    .in(file("modules/producer"))
    .settings(name := "producer")
    .dependsOn(core)
    .enablePlugins(AutomateHeaderPlugin, GitVersioning, ScalafmtCorePlugin)
    .settings(settings)
    .settings(
      libraryDependencies ++=
        Dependencies.compile ++ Dependencies.test
    )

val it =
  project
    .in(file("modules/it"))
    .settings(name := "it")
    .dependsOn(core, producer % "compile->compile;test->test", consumer)
    .enablePlugins(AutomateHeaderPlugin, GitVersioning, ScalafmtCorePlugin)
    .settings(settings)
    .configs(IntegrationTest)
    .settings(Defaults.itSettings: _*)
    .settings(
      libraryDependencies ++=
        Dependencies.compile ++ Dependencies.itTest
    )

val `reactive-kinesis` =
  project
    .in(file("."))
    .settings(name := "reactive-kinesis")
    .aggregate(core, consumer, producer)
    .settings(settings)
    .settings(
      Defaults.coreDefaultSettings ++ Seq(
        publishArtifact := false,
        publishLocal := {},
        publish := {},
        bintrayRelease := {},
        bintraySyncMavenCentral := {}
      )
    )
    .enablePlugins(GitVersioning)

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
commonSettings ++
headerSettings ++
VersionSettings.versioningSettings

lazy val commonSettings =
  Seq(
    //version := "0.1.14", //automatically calculated by sbt-git
    //scalaVersion := "2.11.11", // taken from .travis.yml via sbt-travisci
    organization := "com.weightwatchers",
    mappings.in(Compile, packageBin) += baseDirectory.in(ThisBuild).value / "LICENSE" -> "LICENSE",
    scalacOptions ++= ScalacFlags.all,
    scalacOptions in (Compile, doc) ++= Seq(
      "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
    ),
    scalacOptions in (Compile, console) ~= (_.filterNot(
      Set(
        "-Ywarn-unused:imports",
        "-Xfatal-warnings"
      )
    )),
    unmanagedSourceDirectories.in(Compile) := Seq(scalaSource.in(Compile).value),
    unmanagedSourceDirectories.in(Test) := Seq(scalaSource.in(Test).value),
    shellPrompt in ThisBuild := { state =>
      val project = Project.extract(state).currentRef.project
      s"[$project]> "
    },
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    fork in IntegrationTest := true,
    javaOptions in IntegrationTest += "-Dcom.amazonaws.sdk.disableCertChecking=true",
    envVars in IntegrationTest += ("AWS_CBOR_DISABLE" -> "true"),
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.3.0"
  )

import sbt.Keys.parallelExecution

lazy val headerSettings =
  Seq(
    headerLicense := Some(HeaderLicense.ALv2("2017", "WeightWatchers"))
  )

coverageExcludedPackages := "reference.conf"
