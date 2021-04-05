/*
 * Copyright (2020) The Delta Lake Project Authors.
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

name := "delta-core"

organization := "io.delta"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  // Adding test classifier seems to break transitive resolution of the core dependencies
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",

  // Test deps
  "org.scalatest" %% "scalatest" % "3.0.9" % "test",
  "junit" % "junit" % "4.13.2" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "test" classifier "tests",
)

enablePlugins(Antlr4Plugin)
inConfig(Antlr4) {
  Seq(
    antlr4Version := "4.9.2",
    antlr4PackageName := Some("io.delta.sql.parser"),
    antlr4GenListener := true,
    antlr4GenVisitor := true,
    antlr4TreatWarningsAsErrors := true
  )
}

inConfig(Test) {
  Seq(
    testOptions += Tests.Argument("-oDF"),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    // Don't execute in parallel since we can't have multiple Sparks in the same JVM
    parallelExecution := false,
    // FIXME Is this really used with parallelExecution false?
    fork := true,

    // Configurations to speed up tests and reduce memory footprint
    javaOptions ++= Seq(
      "-Dspark.ui.enabled=false",
      "-Dspark.ui.showConsoleProgress=false",
      "-Dspark.databricks.delta.snapshotPartitions=2",
      "-Dspark.sql.shuffle.partitions=5",
      "-Ddelta.log.cacheSize=3",
      "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
      "-Xmx1024m"
    )
  )
}

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-P:genjavadoc:strictVisibility=true" // hide package private types and methods in javadoc
)

javaOptions += "-Xmx1024m"

/** ********************
 * ScalaStyle settings *
 * *********************/

scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := (Compile / scalastyle).toTask("").value
(Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value

lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := (Test / scalastyle).toTask("").value
(Test / test) := ((Test / test) dependsOn testScalastyle).value

/*********************
 *  MIMA settings    *
 *********************/

(Test / test) := ((Test / test) dependsOn mimaReportBinaryIssues).value

def getVersion(version: String): String = {
    version.split("\\.").toList match {
        case major :: minor :: rest =>
          if (rest.head.startsWith("0")) s"$major.${minor.toInt - 1}.0"
          else s"$major.$minor.${rest.head.replaceAll("-SNAPSHOT", "").toInt - 1}"
        case _ => throw new Exception(s"Could not find previous version for $version.")
    }
}

mimaPreviousArtifacts := Set("io.delta" %% "delta-core" %  getVersion(version.value))
mimaBinaryIssueFilters ++= MimaExcludes.ignoredABIProblems


/*******************
 * Unidoc settings *
 *******************/

enablePlugins(GenJavadocPlugin, JavaUnidocPlugin, ScalaUnidocPlugin)
unidocGenjavadocVersion := "0.17"

// Configure Scala unidoc
ScalaUnidoc / unidoc / scalacOptions ++= Seq(
  "-skip-packages", "org:com:io.delta.sql:io.delta.tables.execution",
  "-doc-title", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
)

// Configure Java unidoc
JavaUnidoc / unidoc / javacOptions := Seq(
  "-public",
  "-exclude", "org:com:io.delta.sql:io.delta.tables.execution",
  "-windowtitle", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
  "-noqualifier", "java.lang",
  "-tag", "return:X",
  // `doclint` is disabled on Circle CI. Need to enable it manually to test our javadoc.
  "-Xdoclint:all"
)

// Explicitly remove source files by package
// these docs are not formatted correctly for Javadocs
def ignoreUndocumentedPackages(packages: Seq[Seq[java.io.File]]): Seq[Seq[java.io.File]] = {
  packages
    .map(_.filterNot(_.getName.contains("$")))
    .map(_.filterNot(_.getCanonicalPath.contains("io/delta/sql")))
    .map(_.filterNot(_.getCanonicalPath.contains("io/delta/tables/execution")))
    .map(_.filterNot(_.getCanonicalPath.contains("spark")))
}

JavaUnidoc / unidoc / unidocAllSources := {
  ignoreUndocumentedPackages((JavaUnidoc / unidoc / unidocAllSources).value)
}

// Ensure unidoc is run with tests
Test / test := ((Test / test) dependsOn (Compile / unidoc)).value

/********************
 * Release settings *
 ********************/

publishMavenStyle := true

releaseCrossBuild := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

pomExtra :=
  <url>https://delta.io/</url>
    <scm>
      <url>git@github.com:delta-io/delta.git</url>
      <connection>scm:git:git@github.com:delta-io/delta.git</connection>
    </scm>
    <developers>
      <developer>
        <id>marmbrus</id>
        <name>Michael Armbrust</name>
        <url>https://github.com/marmbrus</url>
      </developer>
      <developer>
        <id>brkyvz</id>
        <name>Burak Yavuz</name>
        <url>https://github.com/brkyvz</url>
      </developer>
      <developer>
        <id>jose-torres</id>
        <name>Jose Torres</name>
        <url>https://github.com/jose-torres</url>
      </developer>
      <developer>
        <id>liwensun</id>
        <name>Liwen Sun</name>
        <url>https://github.com/liwensun</url>
      </developer>
      <developer>
        <id>mukulmurthy</id>
        <name>Mukul Murthy</name>
        <url>https://github.com/mukulmurthy</url>
      </developer>
      <developer>
        <id>tdas</id>
        <name>Tathagata Das</name>
        <url>https://github.com/tdas</url>
      </developer>
      <developer>
        <id>zsxwing</id>
        <name>Shixiong Zhu</name>
        <url>https://github.com/zsxwing</url>
      </developer>
    </developers>

import ReleaseTransformations._
import com.simplytyped.Antlr4Plugin

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)
