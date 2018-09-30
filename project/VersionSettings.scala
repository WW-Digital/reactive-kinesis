import com.typesafe.sbt.GitPlugin.autoImport.git
import sbt._

object VersionSettings {

  /* This allows to derive an sbt version string from the git information.
   * The logic goes as follows :
   *
   * IF the current commit is tagged with "vX.Y.Z" (ie semantic-versioning), the version is "X.Y.Z"
   * ELSE IF the current commit is tagged with "vX.Y.Z-Mx", the version is "X.Y.Z-Mx"
   * ELSE IF the latest found tag is "vX.Y.Z", the version is "X.Y.Z-commitsSinceVersion-gCommitHash-SNAPSHOT"
   * ELSE the version is "0.0.0-commitHash-SNAPSHOT"
   */
  private val VersionRegex   = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r
  private val MilestoneRegex = "^M[0-9]$".r

  val versioningSettings =
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

}

