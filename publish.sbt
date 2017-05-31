
publishTo := {
  if (isSnapshot.value){
    Some("Artifactory Realm" at "http://oss.jfrog.org/artifactory/oss-snapshot-local")
  }
  else {
    publishTo.value
  }
}

credentials ++= List(Path.userHome / ".bintray" / ".artifactory").filter(_.exists).map(Credentials(_)) //For snapshots

organization := "com.weightwatchers"
pomIncludeRepository := { _ => false } //remove optional dependencies from our pom
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))
homepage := Some(url("http://www.weightwatchers.com"))
scmInfo := Some(ScmInfo(url("https://github.com/WW-Digital/reactive-kinesis"), "scm:git@github.com:WW-Digital/reactive-kinesis.git"))
developers := List(
  Developer("markglh", "Mark Harrison", "markglh@gmail.com", url("https://github.com/markglh")),
  Developer("felixt-cake", "Felix Terkhorn", "felixt@cakesolutions.net", url("https://github.com/felixt-cake")),
  Developer("DavidDeCoding", "David De", "david.de@weightwatchers.com", url("https://github.com/DavidDeCoding")),
  Developer("agaro1121", "Anthony Garo", "anthony.garo@weightwatchers.com", url("https://github.com/agaro1121")),
  Developer("dluwtw", "David Lu", "david.lu@weightwatchers.com", url("https://github.com/dluwtw")))
publishArtifact in Test := false
bintrayReleaseOnPublish := false //We're releasing via travis, set to true to automatically release on publish instead
publishMavenStyle := true
bintrayRepository := "oss"
bintrayOrganization in bintray := None
