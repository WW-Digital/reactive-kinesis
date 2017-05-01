
publishTo := {
  if (isSnapshot.value){
    Some("Artifactory Realm" at "http://oss.jfrog.org/artifactory/oss-snapshot-local")
  }
  else {
    publishTo.value
  }
}

credentials ++= List(Path.userHome / ".bintray" / ".artifactory").filter(_.exists).map(Credentials(_))
organization := "com.weightwatchers"
pomIncludeRepository := { _ => false } //remove optional dependencies from our pom
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))
homepage := Some(url("http://www.weightwatchers.com"))
scmInfo := Some(ScmInfo(url("https://github.com/WW-Digital/reactive-kinesis"), "scm:git@github.com:WW-Digital/reactive-kinesis.git"))
developers := List(Developer("markglh", "Mark Harrison", "markglh@gmail.com", url("https://github.com/markglh")))
publishArtifact in Test := false
bintrayReleaseOnPublish := false //We're releasing via travis, set to true to automatically release on publish instead
publishMavenStyle := true
bintrayRepository := "oss"
bintrayOrganization in bintray := None
