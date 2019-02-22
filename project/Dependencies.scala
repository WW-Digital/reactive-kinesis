import sbt._

// *****************************************************************************
// Library dependencies
// *****************************************************************************
object Dependencies {

  object Version {
    val scalaCheck = "1.13.5"
    val scalaTest  = "3.0.5"
    val jackson    = "2.9.8"
    val akka       = "2.5.19"
    val mockito = "2.16.0"
  }

  private val jackson = Seq(
    //We need jackson versions to be consistent, KCL&KPL pull in slightly older versions which often get evicted
    //See: https://github.com/aws/aws-sdk-java/issues/999
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % Version.jackson % Compile,
    "com.fasterxml.jackson.core"       % "jackson-databind"        % Version.jackson % Compile,
    "com.fasterxml.jackson.core"       % "jackson-core"            % Version.jackson % Compile,
    "com.fasterxml.jackson.core"       % "jackson-annotations"     % Version.jackson % Compile,
    "com.fasterxml.uuid"               % "java-uuid-generator"     % "3.1.5"         % Compile
  )

  val amazonKcl =
    Seq(
      // TODO: Upgrade this to 1.9.x when this issue is resolved and exposed in localstack:
      // https://github.com/mhart/kinesalite/issues/59
      // 1.9.3 breaks KinesisSourceGraphStageIntegrationSpec and ConsumerProcessingManagerIntegrationSpec
      "com.amazonaws" % "amazon-kinesis-client" % "1.8.10" % Compile
        excludeAll (ExclusionRule(organization = "com.fasterxml.jackson.core"),
        ExclusionRule(organization = "com.fasterxml.jackson.dataformat"))
    )

  val amazonKpl =
    Seq(
      "com.amazonaws" % "amazon-kinesis-producer" % "0.12.11" % Compile
        excludeAll (ExclusionRule(organization = "com.fasterxml.jackson.core"),
        ExclusionRule(organization = "com.fasterxml.jackson.dataformat"))
    )

  /*
  * These libs are included with the Amazon dependencies
  * Our core module does not include amazon dependencies so we need to bring them in manually
  * These versions match the ones in the Amazon dependencies
  * */
  private val jodaTime = "joda-time" % "joda-time" % "2.8.1" % Provided
  private val googleGuava = "com.google.guava" % "guava" % "18.0" % Provided
  val libsThatUsedToComeFromAmazon = Seq(jodaTime, googleGuava)

  private val lightbend = Seq(
    "com.typesafe"               % "config"         % "1.3.3"      % Compile,
    "com.typesafe.akka"          %% "akka-actor"    % Version.akka % Compile,
    "com.typesafe.akka"          %% "akka-stream"   % Version.akka % Compile,
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"      % Compile
  )

  private val logback = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3" % Compile
  )

  private val testBase: Seq[ModuleID] = Seq(
    "org.scalatest"     %% "scalatest"    % Version.scalaTest,
    "org.scalacheck"    %% "scalacheck"   % Version.scalaCheck,
    "com.typesafe.akka" %% "akka-testkit" % Version.akka,
    "org.mockito"       % "mockito-core"  % Version.mockito
  )

  val test: Seq[ModuleID] = testBase.map(_ % "test")

  val itTest: Seq[ModuleID] = testBase.map(_ % "it,test")

  val compile: Seq[ModuleID] = jackson ++ lightbend ++ logback


}
