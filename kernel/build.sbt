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

import ReleaseTransformations._

val scala212 = "2.12.15"
scalaVersion := scala212

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := scala212,
  fork := true,
  scalacOptions ++= Seq("-target:jvm-1.8", "-Ywarn-unused:imports"),
  javacOptions ++= Seq("-source", "1.8"),
  // -target cannot be passed as a parameter to javadoc. See https://github.com/sbt/sbt/issues/355
  Compile / compile / javacOptions ++= Seq("-target", "1.8", "-Xlint:unchecked"),
  // Configurations to speed up tests and reduce memory footprint
  Test / javaOptions += "-Xmx1024m",
)

// TODO javastyle checkstyle tests
// TODO unidoc/javadoc settings

lazy val kernelApi = (project in file("kernel-api"))
  .settings(
    name := "delta-kernel-api",
    commonSettings,
    releaseSettings,
    scalaStyleSettings,
    libraryDependencies ++= Seq()
  )

val hadoopVersion = "3.3.1"
val deltaStorageVersion = "2.2.0"
val scalaTestVersion = "3.2.15"
val deltaSparkVersion = deltaStorageVersion
val sparkVersion = "3.3.2"

lazy val kernelDefault = (project in file("kernel-default"))
  .dependsOn(kernelApi)
  .settings(
    name := "delta-kernel-default",
    commonSettings,
    releaseSettings,
    scalaStyleSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion, // Configuration, Path
      "io.delta" % "delta-storage" % deltaStorageVersion, // LogStore
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5", // ObjectMapper
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3",

      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "io.delta" %% "delta-core" % deltaSparkVersion % "test",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test", // SparkSession
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
      "junit" % "junit" % "4.11" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
    )
  )

/*
 ***********************
 * ScalaStyle settings *
 ***********************
 */
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

// Not used since scala is test-only
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val scalaStyleSettings = Seq(
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  Compile / compile := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  Test / test := ((Test / test) dependsOn testScalastyle).value
)

/*
 ********************
 * Release settings *
 ********************
 */

// Looks some of release settings should be set for the root project as well.
publishArtifact := false  // Don't release the root project
publish / skip := true
publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
releaseCrossBuild := false
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  setNextVersion,
  commitNextVersion
)

/**
 * Release settings for artifact that contains only Java source code
 */
lazy val javaOnlyReleaseSettings = Seq(
  // drop off Scala suffix from artifact names
  crossPaths := false,
  // we publish jars for each scalaVersion in crossScalaVersions. however, we only need to publish
  // one java jar. thus, only do so when the current scala version == default scala version
  publishArtifact := scalaBinaryVersion.value == "2.12",
  // exclude scala-library from dependencies in generated pom.xml
  autoScalaLibrary := false,
)

lazy val releaseSettings = Seq(
  publishMavenStyle := true,
  publishArtifact := true,
  Test / publishArtifact := false,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseCrossBuild := true,
  pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toArray),
  sonatypeProfileName := "io.delta", // sonatype account domain name prefix / group ID
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("SONATYPE_USERNAME", ""),
    sys.env.getOrElse("SONATYPE_PASSWORD", "")
  ),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) {
      Some("snapshots" at nexus + "content/repositories/snapshots")
    } else {
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  },
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  pomExtra :=
    <url>https://github.com/delta-io/connectors</url>
      <scm>
        <url>git@github.com:delta-io/connectors.git</url>
        <connection>scm:git:git@github.com:delta-io/connectors.git</connection>
      </scm>,
) ++ javaOnlyReleaseSettings

/*
 ********************
 *  MIMA settings   *
 ********************
 */
def getPrevVersion(currentVersion: String): String = {
  implicit def extractInt(str: String): Int = {
    """\d+""".r.findFirstIn(str).map(java.lang.Integer.parseInt).getOrElse {
      throw new Exception(s"Could not extract version number from $str in $version")
    }
  }

  val (major, minor, patch): (Int, Int, Int) = {
    currentVersion.split("\\.").toList match {
      case majorStr :: minorStr :: patchStr :: _ =>
        (majorStr, minorStr, patchStr)
      case _ => throw new Exception(s"Could not find previous version for $version.")
    }
  }

  val majorToLastMinorVersions: Map[Int, Int] = Map(
    // TODO add mapping when required
    // e.g. 0 -> 8
  )
  if (minor == 0) {  // 1.0.0
    val prevMinor = majorToLastMinorVersions.getOrElse(major - 1, {
      throw new Exception(s"Last minor version of ${major - 1}.x.x not configured.")
    })
    s"${major - 1}.$prevMinor.0"  // 1.0.0 -> 0.8.0
  } else if (patch == 0) {
    s"$major.${minor - 1}.0"      // 1.1.0 -> 1.0.0
  } else {
    s"$major.$minor.${patch - 1}" // 1.1.1 -> 1.1.0
  }
}

lazy val mimaSettings = Seq(
  (Test / test) := ((Test / test) dependsOn mimaReportBinaryIssues).value,
  mimaPreviousArtifacts := Set.empty // TODO update this after first release and activate
  // mimaBinaryIssueFilters ++= MimaExcludes.ignoredABIProblems // TODO update when added
)
