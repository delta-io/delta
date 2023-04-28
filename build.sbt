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

import java.nio.file.Files
import TestParallelization._

val sparkVersion = "3.3.2"
val scala212 = "2.12.15"
val scala213 = "2.13.5"
val default_scala_version = scala212
val all_scala_versions = Seq(scala212, scala213)

scalaVersion := default_scala_version

// crossScalaVersions must be set to Nil on the root project
crossScalaVersions := Nil

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := default_scala_version,
  crossScalaVersions := all_scala_versions,
  fork := true,
  scalacOptions ++= Seq("-target:jvm-1.8", "-Ywarn-unused:imports"),
  javacOptions ++= Seq("-source", "1.8"),
  // -target cannot be passed as a parameter to javadoc. See https://github.com/sbt/sbt/issues/355
  Compile / compile / javacOptions ++= Seq("-target", "1.8")
)

lazy val core = (project in file("core"))
  .dependsOn(storage)
  .enablePlugins(GenJavadocPlugin, JavaUnidocPlugin, ScalaUnidocPlugin, Antlr4Plugin)
  .settings (
    name := "delta-core",
    commonSettings,
    scalaStyleSettings,
    mimaSettings,
    unidocSettings,
    releaseSettings,
    libraryDependencies ++= Seq(
      // Adding test classifier seems to break transitive resolution of the core dependencies
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",

      // Test deps
      "org.scalatest" %% "scalatest" % "3.2.9" % "test",
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test",
      "junit" % "junit" % "4.12" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "test" classifier "tests",

      // Compiler plugins
      // -- Bump up the genjavadoc version explicitly to 0.18 to work with Scala 2.12
      compilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.18" cross CrossVersion.full)
    ),
    Compile / packageBin / mappings := (Compile / packageBin / mappings).value ++
        listPythonFiles(baseDirectory.value.getParentFile / "python"),

    Antlr4 / antlr4Version:= "4.8",
    Antlr4 / antlr4PackageName := Some("io.delta.sql.parser"),
    Antlr4 / antlr4GenListener := true,
    Antlr4 / antlr4GenVisitor := true,

    Test / testOptions += Tests.Argument("-oDF"),
    Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

    // Don't execute in parallel since we can't have multiple Sparks in the same JVM
    Test / parallelExecution := false,

    scalacOptions ++= Seq(
      "-P:genjavadoc:strictVisibility=true" // hide package private types and methods in javadoc
    ),

    javaOptions += "-Xmx1024m",

    // Configurations to speed up tests and reduce memory footprint
    Test / javaOptions ++= Seq(
      "-Dspark.ui.enabled=false",
      "-Dspark.ui.showConsoleProgress=false",
      "-Dspark.databricks.delta.snapshotPartitions=2",
      "-Dspark.sql.shuffle.partitions=5",
      "-Ddelta.log.cacheSize=3",
      "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
      "-Xmx1024m"
    ),

    // Required for testing table features see https://github.com/delta-io/delta/issues/1602
    Test / envVars += ("DELTA_TESTING", "1"),

    // Hack to avoid errors related to missing repo-root/target/scala-2.12/classes/
    createTargetClassesDir := {
      val dir = baseDirectory.value.getParentFile / "target" / "scala-2.12" / "classes"
      Files.createDirectories(dir.toPath)
    },
    Compile / compile := ((Compile / compile) dependsOn createTargetClassesDir).value,
    // Generate the package object to provide the version information in runtime.
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "io" / "delta" / "package.scala"
      IO.write(file,
        s"""package io
           |
           |package object delta {
           |  val VERSION = "${version.value}"
           |}
           |""".stripMargin)
      Seq(file)
    },
    TestParallelization.settings,
  )

lazy val contribs = (project in file("contribs"))
  .dependsOn(core % "compile->compile;test->test;provided->provided")
  .settings (
    name := "delta-contribs",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    Compile / packageBin / mappings := (Compile / packageBin / mappings).value ++
      listPythonFiles(baseDirectory.value.getParentFile / "python"),

    Test / testOptions += Tests.Argument("-oDF"),
    Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

    // Don't execute in parallel since we can't have multiple Sparks in the same JVM
    Test / parallelExecution := false,

    javaOptions += "-Xmx1024m",

    // Configurations to speed up tests and reduce memory footprint
    Test / javaOptions ++= Seq(
      "-Dspark.ui.enabled=false",
      "-Dspark.ui.showConsoleProgress=false",
      "-Dspark.databricks.delta.snapshotPartitions=2",
      "-Dspark.sql.shuffle.partitions=5",
      "-Ddelta.log.cacheSize=3",
      "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
      "-Xmx1024m"
    ),

    // Hack to avoid errors related to missing repo-root/target/scala-2.12/classes/
    createTargetClassesDir := {
      val dir = baseDirectory.value.getParentFile / "target" / "scala-2.12" / "classes"
      Files.createDirectories(dir.toPath)
    },
    Compile / compile := ((Compile / compile) dependsOn createTargetClassesDir).value
  )

// TODO javastyle tests
// TODO unidoc
// TODO(scott): figure out a better way to include tests in this project
lazy val storage = (project in file("storage"))
  .settings (
    name := "delta-storage",
    commonSettings,
    javaOnlyReleaseSettings,
    libraryDependencies ++= Seq(
      // User can provide any 2.x or 3.x version. We don't use any new fancy APIs. Watch out for
      // versions with known vulnerabilities.
      "org.apache.hadoop" % "hadoop-common" % "3.3.1" % "provided",

      // Note that the org.apache.hadoop.fs.s3a.Listing::createFileStatusListingIterator 3.3.1 API
      // is not compatible with 3.3.2.
      "org.apache.hadoop" % "hadoop-aws" % "3.3.1" % "provided",

      // Test Deps
      "org.scalatest" %% "scalatest" % "3.2.11" % "test",
    )
  )

lazy val storageS3DynamoDB = (project in file("storage-s3-dynamodb"))
  .dependsOn(storage % "compile->compile;test->test;provided->provided")
  .dependsOn(core % "test->test")
  .settings (
    name := "delta-storage-s3-dynamodb",
    commonSettings,
    javaOnlyReleaseSettings,

    // uncomment only when testing FailingS3DynamoDBLogStore. this will include test sources in
    // a separate test jar.
    // Test / publishArtifact := true,

    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "provided",

      // Test Deps
      "org.apache.hadoop" % "hadoop-aws" % "3.3.1" % "test", // RemoteFileChangedException
    )
  )

lazy val deltaIceberg = (project in file("delta-iceberg"))
  .dependsOn(core % "compile->compile;test->test;provided->provided")
  .settings (
    name := "delta-iceberg",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    libraryDependencies ++= Seq( {
        val (expMaj, expMin, _) = getMajorMinorPatch(sparkVersion)
        ("org.apache.iceberg" % s"iceberg-spark-runtime-$expMaj.$expMin" % "1.1.0" % "provided")
          .cross(CrossVersion.binary)
      },
      // Fix Iceberg's legacy java.lang.NoClassDefFoundError: scala/jdk/CollectionConverters$ error
      // due to legacy scala.
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.1"
    )
  )

/**
 * Get list of python files and return the mapping between source files and target paths
 * in the generated package JAR.
 */
def listPythonFiles(pythonBase: File): Seq[(File, String)] = {
  val pythonExcludeDirs = pythonBase / "lib" :: pythonBase / "doc" :: pythonBase / "bin" :: Nil
  import scala.collection.JavaConverters._
  val pythonFiles = Files.walk(pythonBase.toPath).iterator().asScala
    .map { path => path.toFile() }
    .filter { file => file.getName.endsWith(".py") && ! file.getName.contains("test") }
    .filter { file => ! pythonExcludeDirs.exists { base => IO.relativize(base, file).nonEmpty} }
    .toSeq

  pythonFiles pair Path.relativeTo(pythonBase)
}

ThisBuild / parallelExecution := false

val createTargetClassesDir = taskKey[Unit]("create target classes dir")

/*
 ***********************
 * ScalaStyle settings *
 ***********************
 */
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

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
 *  MIMA settings   *
 ********************
 */

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

def getPrevVersion(currentVersion: String): String = {
  val (major, minor, patch) = getMajorMinorPatch(currentVersion)

  val lastVersionInMajorVersion = Map(
    0 -> "0.8.0",
    1 -> "1.2.1"
  )
  if (minor == 0) {  // 1.0.0 or 2.0.0
    lastVersionInMajorVersion.getOrElse(major - 1, {
      throw new Exception(s"Last version of ${major - 1}.x.x not configured.")
    })
  } else if (patch == 0) {
    s"$major.${minor - 1}.0"      // 1.1.0 -> 1.0.0
  } else {
    s"$major.$minor.${patch - 1}" // 1.1.1 -> 1.1.0
  }
}

lazy val mimaSettings = Seq(
  Test / test := ((Test / test) dependsOn mimaReportBinaryIssues).value,
  mimaPreviousArtifacts := Set("io.delta" %% "delta-core" %  getPrevVersion(version.value)),
  mimaBinaryIssueFilters ++= MimaExcludes.ignoredABIProblems
)

/*
 *******************
 * Unidoc settings *
 *******************
 */

// Explicitly remove source files by package because these docs are not formatted well for Javadocs
def ignoreUndocumentedPackages(packages: Seq[Seq[java.io.File]]): Seq[Seq[java.io.File]] = {
  packages
    .map(_.filterNot(_.getName.contains("$")))
    .map(_.filterNot(_.getCanonicalPath.contains("io/delta/sql")))
    .map(_.filterNot(_.getCanonicalPath.contains("io/delta/tables/execution")))
    .map { _.filterNot { f =>
        // LogStore.java and CloseableIterator.java are the only public io.delta.storage APIs
        f.getCanonicalPath.contains("io/delta/storage") &&
        f.getName != "LogStore.java" &&
        f.getName != "CloseableIterator.java"
      }
    }
    .map(_.filterNot(_.getCanonicalPath.contains("spark")))
}

lazy val unidocSettings = Seq(

  // Configure Scala unidoc
  ScalaUnidoc / unidoc / scalacOptions ++= Seq(
    "-skip-packages", "org:com:io.delta.sql:io.delta.tables.execution",
    "-doc-title", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
  ),

  ScalaUnidoc / unidoc / unidocAllSources := {
    (ScalaUnidoc / unidoc / unidocAllSources).value
      // ignore Scala (non-public) io.delta.storage classes
      .map(_.filterNot(_.getCanonicalPath.contains("io/delta/storage"))) ++
    // include public io.delta.storage classes
    (JavaUnidoc / unidoc / unidocAllSources).value
      .map { _.filter { f =>
          f.getCanonicalPath.contains("io/delta/storage") &&
          (f.getName == "LogStore.java" || f.getName == "CloseableIterator.java")
        }
      }
  },

  // Configure Java unidoc
  JavaUnidoc / unidoc / javacOptions := Seq(
    "-public",
    "-exclude", "org:com:io.delta.sql:io.delta.tables.execution",
    "-windowtitle", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
    "-noqualifier", "java.lang",
    "-tag", "return:X",
    // `doclint` is disabled on Circle CI. Need to enable it manually to test our javadoc.
    "-Xdoclint:all"
  ),

  JavaUnidoc / unidoc / unidocAllSources := {
    ignoreUndocumentedPackages((JavaUnidoc / unidoc / unidocAllSources).value)
  },

  // Ensure unidoc is run with tests
  Test / test := ((Test / test) dependsOn (Compile / unidoc)).value
)

/*
 ********************
 * Release settings *
 ********************
 */
import ReleaseTransformations._

lazy val skipReleaseSettings = Seq(
  publishArtifact := false,
  publish / skip := true
)

/**
 * Release settings for artifact that contains only Java source code
 */
lazy val javaOnlyReleaseSettings = releaseSettings ++ Seq(
  // drop off Scala suffix from artifact names
  crossPaths := false,

  // we publish jars for each scalaVersion in crossScalaVersions. however, we only need to publish
  // one java jar. thus, only do so when the current scala version == default scala version
  publishArtifact := {
    val (expMaj, expMin, _) = getMajorMinorPatch(default_scala_version)
    s"$expMaj.$expMin" == scalaBinaryVersion.value
  },

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

  // TODO: This isn't working yet ...
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
)

// Looks like some of release settings should be set for the root project as well.
publishArtifact := false  // Don't release the root project
publish / skip := true
publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
releaseCrossBuild := false  // Don't use sbt-release's cross facility
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  // Do NOT use `sonatypeBundleRelease` - it will actually release to Maven! We want to do that
  // manually.
  //
  // Do NOT use `sonatypePromote` - it will promote the closed staging repository (i.e. sync to
  //                                Maven central)
  //
  // See https://github.com/xerial/sbt-sonatype#publishing-your-artifact.
  //
  // - sonatypePrepare: Drop the existing staging repositories (if exist) and create a new staging
  //                    repository using sonatypeSessionName as a unique key
  // - sonatypeBundleUpload: Upload your local staging folder contents to a remote Sonatype
  //                         repository
  // - sonatypeClose: closes your staging repository at Sonatype. This step verifies Maven central
  //                  sync requirement, GPG-signature, javadoc and source code presence, pom.xml
  //                  settings, etc
  // TODO: this isn't working yet
  // releaseStepCommand("sonatypePrepare; sonatypeBundleUpload; sonatypeClose"),
  setNextVersion,
  commitNextVersion
)
