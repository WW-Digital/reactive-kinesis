// *****************************************************************************
// Projects
// *****************************************************************************

lazy val `reactive-kinesis` =
  project
    .in(file("."))
    .enablePlugins(AutomateHeaderPlugin, GitVersioning, ScalafmtCorePlugin)
    .settings(settings)
    .configs(IntegrationTest)
    .settings(Defaults.itSettings: _*)
    .settings(
      libraryDependencies ++=
        library.jackson ++ library.amazon ++ library.lightbend ++
        library.logback ++ library.testing
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {

    object Version {
      val scalaCheck = "1.13.5"
      val scalaTest  = "3.0.5"
      val jackson    = "2.9.4"
      val akka       = "2.5.11"
    }

    val jackson = Seq(
      //We need jackson versions to be consistent, KCL&KPL pull in slightly older versions which often get evicted
      //See: https://github.com/aws/aws-sdk-java/issues/999
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % Version.jackson % Compile,
      "com.fasterxml.jackson.core"       % "jackson-databind"        % Version.jackson % Compile,
      "com.fasterxml.jackson.core"       % "jackson-core"            % Version.jackson % Compile,
      "com.fasterxml.jackson.core"       % "jackson-annotations"     % Version.jackson % Compile,
      "com.fasterxml.uuid"               % "java-uuid-generator"     % "3.1.5"         % Compile
    )

    val amazon = Seq(
      // TODO: Upgrade this to 1.9.x when this issue is resolved and exposed in localstack:
      // https://github.com/mhart/kinesalite/issues/59
      "com.amazonaws" % "amazon-kinesis-client" % "1.8.10" % Compile
      excludeAll (ExclusionRule(organization = "com.fasterxml.jackson.core"),
      ExclusionRule(organization = "com.fasterxml.jackson.dataformat")),
      "com.amazonaws" % "amazon-kinesis-producer" % "0.12.8" % Compile
      excludeAll (ExclusionRule(organization = "com.fasterxml.jackson.core"),
      ExclusionRule(organization = "com.fasterxml.jackson.dataformat"))
    )

    val lightbend = Seq(
      "com.typesafe"               % "config"         % "1.3.3"      % Compile,
      "com.typesafe.akka"          %% "akka-actor"    % Version.akka % Compile,
      "com.typesafe.akka"          %% "akka-stream"   % Version.akka % Compile,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"      % Compile
    )

    val logback = Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3" % Compile
    )

    val testing = Seq(
      "org.scalatest"     %% "scalatest"    % Version.scalaTest  % "it,test",
      "org.scalacheck"    %% "scalacheck"   % Version.scalaCheck % "it,test",
      "com.typesafe.akka" %% "akka-testkit" % Version.akka       % "it,test",
      "org.mockito"       % "mockito-core"  % "2.16.0"           % "it,test"
    )
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
commonSettings ++
headerSettings ++
versioningSettings

lazy val commonSettings =
  Seq(
    //version := "0.1.14", //automatically calculated by sbt-git
    //scalaVersion := "2.11.11", // taken from .travis.yml via sbt-travisci
    organization := "com.weightwatchers",
    mappings.in(Compile, packageBin) += baseDirectory.in(ThisBuild).value / "LICENSE" -> "LICENSE",
    scalacOptions ++= Seq( //http://tpolecat.github.io/2017/04/25/scalac-flags.html
      "-deprecation", // Emit warning and location for usages of deprecated APIs.
      "-encoding",
      "utf-8", // Specify character encoding used by source files.
      "-explaintypes", // Explain type errors in more detail.
      "-feature", // Emit warning and location for usages of features that should be imported explicitly.
      "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
      "-language:experimental.macros", // Allow macro definition (besides implementation and application)
      "-language:higherKinds", // Allow higher-kinded types
      "-language:implicitConversions", // Allow definition of implicit functions called views
      "-unchecked", // Enable additional warnings where generated code depends on assumptions.
      "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
      "-Xfatal-warnings", // Fail the compilation if there are any warnings.
      "-Xfuture", // Turn on future language features.
      "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
      "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
      "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
      "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
      "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
      "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
      "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
      "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
      "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
      "-Xlint:option-implicit", // Option.apply used implicit view.
      "-Xlint:package-object-classes", // Class or object defined in package object.
      "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
      "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
      "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
      "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
      "-Xlint:unsound-match", // Pattern match may not be typesafe.
      "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
      "-Ypartial-unification", // Enable partial unification in type constructor inference
      "-Ywarn-dead-code", // Warn when dead code is identified.
      "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
      "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
      "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
      "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
      "-Ywarn-numeric-widen" // Warn when numerics are widened.
    ),
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

/* This allows to derive an sbt version string from the git information.
 * The logic goes as follows :
 *
 * IF the current commit is tagged with "vX.Y.Z" (ie semantic-versioning), the version is "X.Y.Z"
 * ELSE IF the current commit is tagged with "vX.Y.Z-Mx", the version is "X.Y.Z-Mx"
 * ELSE IF the latest found tag is "vX.Y.Z", the version is "X.Y.Z-commitsSinceVersion-gCommitHash-SNAPSHOT"
 * ELSE the version is "0.0.0-commitHash-SNAPSHOT"
 */
val VersionRegex   = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r
val MilestoneRegex = "^M[0-9]$".r
lazy val versioningSettings =
  Seq(
    git.baseVersion := "0.0.0",
    git.useGitDescribe := true,
    git.uncommittedSignifier := None,
    git.gitTagToVersionNumber := {
      case VersionRegex(v, "") => Some(v) //e.g. 1.0.0
      case VersionRegex(v, s) if MilestoneRegex.findFirstIn(s).isDefined =>
        Some(s"$v-$s") //e.g. 1.0.0-M1
      case VersionRegex(v, "SNAPSHOT") => Some(s"$v-SNAPSHOT") //e.g. 1.0.0-SNAPSHOT
      case VersionRegex(v, s)          => Some(s"$v-$s-SNAPSHOT") //e.g. 1.0.0-2-commithash-SNAPSHOT
      case _                           => None
    }
  )

import sbt.Keys.parallelExecution

import scala.io.Source

lazy val headerSettings =
  Seq(
    headerLicense := Some(HeaderLicense.Custom(Source.fromFile("LICENSE").mkString))
  )

coverageExcludedPackages := "reference.conf"
