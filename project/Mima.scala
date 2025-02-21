/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.typesafe.tools.mima.plugin.MimaPlugin.autoImport.{mimaBinaryIssueFilters, mimaPreviousArtifacts, mimaReportBinaryIssues}
import sbt._
import sbt.Keys._

/**
 * Mima settings
 */
object Mima {

  /**
   * @return tuple of (major, minor, patch) versions extracted from a version string.
   *         e.g. "1.2.3" would return (1, 2, 3)
   */
  def getMajorMinorPatch(versionStr: String): (Int, Int, Int) = {
    implicit def extractInt(str: String): Int = {
      """\d+""".r.findFirstIn(str).map(java.lang.Integer.parseInt).getOrElse {
        throw new Exception(s"Could not extract version number from $str in $version")
      }
    }

    versionStr.split("\\.").toList match {
      case majorStr :: minorStr :: patchStr :: _ =>
        (majorStr, minorStr, patchStr)
      case _ => throw new Exception(s"Could not parse version for $version.")
    }
  }

  def getPrevSparkName(currentVersion: String): String = {
    val prevSparkVersion = getPrevSparkVersion(currentVersion)

    val (major, minor, patch) = getMajorMinorPatch(prevSparkVersion)

    if (major >= 3) "delta-spark" else "delta-core"
  }

  def getPrevSparkVersion(currentVersion: String): String = {
    val (major, minor, patch) = getMajorMinorPatch(currentVersion)

    val lastVersionInMajorVersion = Map(
      0 -> "0.8.0",
      1 -> "1.2.1",
      2 -> "2.4.0"
    )
    if (minor == 0) {  // 1.0.0 or 2.0.0 or 3.0.0
      lastVersionInMajorVersion.getOrElse(major - 1, {
        throw new Exception(s"Last version of ${major - 1}.x.x not configured.")
      })
    } else if (patch == 0) {
      s"$major.${minor - 1}.0"      // 1.1.0 -> 1.0.0
    } else {
      s"$major.$minor.${patch - 1}" // 1.1.1 -> 1.1.0
    }
  }

  def getPrevConnectorVersion(currentVersion: String): String = {
    val (major, minor, patch) = getMajorMinorPatch(currentVersion)

    val majorToLastMinorVersions: Map[Int, String] = Map(
      // We skip from 0.6.0 to 3.0.0 when migrating connectors to the main delta repo
      0 -> "0.6.0",
      1 -> "0.6.0",
      2 -> "0.6.0"
    )
    if (minor == 0) {  // 1.0.0
      majorToLastMinorVersions.getOrElse(major - 1, {
        throw new Exception(s"Last minor version of ${major - 1}.x.x not configured.")
      })
    } else if (patch == 0) {
      s"$major.${minor - 1}.0"      // 1.1.0 -> 1.0.0
    } else {
      s"$major.$minor.${patch - 1}" // 1.1.1 -> 1.1.0
    }
  }

  lazy val sparkMimaSettings = Seq(
    Test / test := ((Test / test) dependsOn mimaReportBinaryIssues).value,
    mimaPreviousArtifacts :=
      Set("io.delta" %% getPrevSparkName(version.value) %  getPrevSparkVersion(version.value)),
    mimaBinaryIssueFilters ++= SparkMimaExcludes.ignoredABIProblems
  )

  lazy val standaloneMimaSettings = Seq(
    Test / test := ((Test / test) dependsOn mimaReportBinaryIssues).value,
    mimaPreviousArtifacts := {
      Set("io.delta" %% "delta-standalone" % getPrevConnectorVersion(version.value))
    },
    mimaBinaryIssueFilters ++= StandaloneMimaExcludes.ignoredABIProblems
  )

  lazy val flinkMimaSettings = Seq(
    Test / test := ((Test / test) dependsOn mimaReportBinaryIssues).value,
    mimaPreviousArtifacts := {
      Set("io.delta" % "delta-flink" % getPrevConnectorVersion(version.value))
    },
    mimaBinaryIssueFilters ++= FlinkMimaExcludes.ignoredABIProblems
  )
}
