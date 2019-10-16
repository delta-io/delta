/*
 * Copyright 2019 Databricks, Inc.
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

crossScalaVersions := Seq("2.12.8", "2.11.12")

scalaVersion := crossScalaVersions.value.head

sparkVersion := "2.4.2"

lazy val hive = (project in file("external/hive-delta")) settings (
  scalaVersion := "2.11.12",

  libraryDependencies ++= Seq(
    // Hive 2.3.6 is using Parquet 1.8.1 but Spark 2.4.2 cannot use Parquet 1.8.1 to write.
    // We lock down the Parquet version to 1.10.1 for now. Note: we need to declare them here,
    // otherwise, SBT will go through the spark dependency tree and use the first one it found which
    // is Parquet 1.8.1
    // TODO: Check if files written by Parquet 1.10.1 can be read by Parquet 1.8.1, or  use
    //  Hive 3.1.0
    "org.apache.parquet" % "parquet-common" % "1.10.1" % "provided",
    "org.apache.parquet" % "parquet-hadoop" % "1.10.1" % "provided",
    "org.apache.parquet" % "parquet-column" % "1.10.1" % "provided",
    "org.apache.parquet" % "parquet-format" % "2.4.0" % "provided",
    "io.delta" %% "delta-core" % "0.4.0", // TODO make it depend on the root project
    "org.apache.spark" %% "spark-core" % "2.4.2" % "provided",
    "org.apache.spark" %% "spark-catalyst" % "2.4.2" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.2" % "provided",
    "org.apache.hive" % "hive-exec" % "2.3.3" % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % "2.7.0" % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.7.0" % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % "2.7.0" % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % "2.3.3" % "test" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.apache.spark" %% "spark-core" % "2.4.2" % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
)

libraryDependencies ++= Seq(
  // Adding test classifier seems to break transitive resolution of the core dependencies
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",

  // Test deps
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "junit" % "junit" % "4.12" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests"
)

antlr4Settings

antlr4Version in Antlr4 := "4.7"

antlr4PackageName in Antlr4 := Some("io.delta.sql.parser")

antlr4GenListener in Antlr4 := true

antlr4GenVisitor in Antlr4 := true

testOptions in Test += Tests.Argument("-oDF")

testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

// Don't execute in parallel since we can't have multiple Sparks in the same JVM
parallelExecution in Test := false

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-P:genjavadoc:strictVisibility=true" // hide package private types and methods in javadoc
)

javaOptions += "-Xmx1024m"

fork in Test := true

// Configurations to speed up tests and reduce memory footprint
javaOptions in Test ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Dspark.databricks.delta.snapshotPartitions=2",
  "-Dspark.sql.shuffle.partitions=5",
  "-Ddelta.log.cacheSize=3",
  "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
  "-Xmx1024m"
)

/** ********************
 * ScalaStyle settings *
 * *********************/

scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

lazy val testScalastyle = taskKey[Unit]("testScalastyle")

testScalastyle := scalastyle.in(Test).toTask("").value

(test in Test) := ((test in Test) dependsOn testScalastyle).value

/*********************
 *  MIMA settings    *
 *********************/

(test in Test) := ((test in Test) dependsOn mimaReportBinaryIssues).value

def getVersion(version: String): String = {
    version.split("\\.").toList match {
        case major :: minor :: rest => s"$major.$minor.0" 
        case _ => throw new Exception(s"Could not find previous version for $version.")
    }
}

mimaPreviousArtifacts := Set("io.delta" %% "delta-core" %  getVersion(version.value))
mimaBinaryIssueFilters ++= MimaExcludes.ignoredABIProblems


/*******************
 * Unidoc settings *
 *******************/

enablePlugins(GenJavadocPlugin, JavaUnidocPlugin, ScalaUnidocPlugin)

// Configure Scala unidoc
scalacOptions in(ScalaUnidoc, unidoc) ++= Seq(
  "-skip-packages", "org:com:io.delta.sql:io.delta.tables.execution",
  "-doc-title", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
)

// Configure Java unidoc
javacOptions in(JavaUnidoc, unidoc) := Seq(
  "-public",
  "-exclude", "org:com:io.delta.sql:io.delta.tables.execution",
  "-windowtitle", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
  "-noqualifier", "java.lang",
  "-tag", "return:X",
  // `doclint` is disabled on Circle CI. Need to enable it manually to test our javadoc.
  "-Xdoclint:all"
)

// Explicitly remove source files by package because these docs are not formatted correctly for Javadocs
def ignoreUndocumentedPackages(packages: Seq[Seq[java.io.File]]): Seq[Seq[java.io.File]] = {
  packages
    .map(_.filterNot(_.getName.contains("$")))
    .map(_.filterNot(_.getCanonicalPath.contains("io/delta/sql")))
    .map(_.filterNot(_.getCanonicalPath.contains("io/delta/tables/execution")))
    .map(_.filterNot(_.getCanonicalPath.contains("spark")))
}

unidocAllSources in(JavaUnidoc, unidoc) := {
  ignoreUndocumentedPackages((unidocAllSources in(JavaUnidoc, unidoc)).value)
}

// Ensure unidoc is run with tests
(test in Test) := ((test in Test) dependsOn unidoc.in(Compile)).value


/***************************
 * Spark Packages settings *
 ***************************/

spName := "databricks/delta-core"

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

packagedArtifacts in publishM2 <<= packagedArtifacts in spPublishLocal

packageBin in Compile := spPackage.value

sparkComponents := Seq("sql")

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

bintrayOrganization := Some("delta-io")

bintrayRepository := "delta"

import ReleaseTransformations._

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
