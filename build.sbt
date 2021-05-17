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

import java.nio.file.Files

val sparkVersion = "3.1.1"
scalaVersion := "2.12.10"

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := "2.12.10",
  fork := true
)

lazy val core = (project in file("core"))
  .enablePlugins(GenJavadocPlugin, JavaUnidocPlugin, ScalaUnidocPlugin)
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
      "org.scalatest" %% "scalatest" % "3.1.0" % "test",
      "junit" % "junit" % "4.12" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "test" classifier "tests",

      // Compiler plugins
      // -- Bump up the genjavadoc version explicitly to 0.16 to work with Scala 2.12
      compilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.16" cross CrossVersion.full)
    ),
    (mappings in (Compile, packageBin)) := (mappings in (Compile, packageBin)).value ++
        listPythonFiles(baseDirectory.value.getParentFile / "python"),

    antlr4Settings,
    antlr4Version in Antlr4 := "4.7",
    antlr4PackageName in Antlr4 := Some("io.delta.sql.parser"),
    antlr4GenListener in Antlr4 := true,
    antlr4GenVisitor in Antlr4 := true,

    testOptions in Test += Tests.Argument("-oDF"),
    testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

    // Don't execute in parallel since we can't have multiple Sparks in the same JVM
    parallelExecution in Test := false,

    scalacOptions ++= Seq(
      "-target:jvm-1.8",
      "-P:genjavadoc:strictVisibility=true" // hide package private types and methods in javadoc
    ),

    javaOptions += "-Xmx1024m",

    // Configurations to speed up tests and reduce memory footprint
    javaOptions in Test ++= Seq(
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
    (compile in Compile) := ((compile in Compile) dependsOn createTargetClassesDir).value
  )

lazy val contribs = (project in file("contribs"))
  .dependsOn(core % "compile->compile;test->test;provided->provided")
  .settings (
    name := "delta-contribs",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    (mappings in (Compile, packageBin)) := (mappings in (Compile, packageBin)).value ++
      listPythonFiles(baseDirectory.value.getParentFile / "python"),

    testOptions in Test += Tests.Argument("-oDF"),
    testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

    // Don't execute in parallel since we can't have multiple Sparks in the same JVM
    parallelExecution in Test := false,

    scalacOptions ++= Seq(
      "-target:jvm-1.8"
    ),

    javaOptions += "-Xmx1024m",

    // Configurations to speed up tests and reduce memory footprint
    javaOptions in Test ++= Seq(
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
    (compile in Compile) := ((compile in Compile) dependsOn createTargetClassesDir).value
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
  pythonFiles pair relativeTo(pythonBase)
}

parallelExecution in ThisBuild := false

val createTargetClassesDir = taskKey[Unit]("create target classes dir")

/*
 ***********************
 * ScalaStyle settings *
 ***********************
 */
scalastyleConfig in ThisBuild := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val scalaStyleSettings = Seq(
  compileScalastyle := scalastyle.in(Compile).toTask("").value,

  (compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value,

  testScalastyle := scalastyle.in(Test).toTask("").value,

  (test in Test) := ((test in Test) dependsOn testScalastyle).value
)

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

  val majorToLastMinorVersions = Map(
    0 -> 8
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
  (test in Test) := ((test in Test) dependsOn mimaReportBinaryIssues).value,
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
    .map(_.filterNot(_.getCanonicalPath.contains("spark")))
}

lazy val unidocSettings = Seq(

  // Configure Scala unidoc
  scalacOptions in(ScalaUnidoc, unidoc) ++= Seq(
    "-skip-packages", "org:com:io.delta.sql:io.delta.tables.execution",
    "-doc-title", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
  ),

  // Configure Java unidoc
  javacOptions in(JavaUnidoc, unidoc) := Seq(
    "-public",
    "-exclude", "org:com:io.delta.sql:io.delta.tables.execution",
    "-windowtitle", "Delta Lake " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
    "-noqualifier", "java.lang",
    "-tag", "return:X",
    // `doclint` is disabled on Circle CI. Need to enable it manually to test our javadoc.
    "-Xdoclint:all"
  ),

  unidocAllSources in(JavaUnidoc, unidoc) := {
    ignoreUndocumentedPackages((unidocAllSources in(JavaUnidoc, unidoc)).value)
  },

  // Ensure unidoc is run with tests
  (test in Test) := ((test in Test) dependsOn unidoc.in(Compile)).value
)

/*
 ********************
 * Release settings *
 ********************
 */
import ReleaseTransformations._

lazy val releaseSettings = Seq(
  publishMavenStyle := true,

  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) {
      Some("snapshots" at nexus + "content/repositories/snapshots")
    } else {
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  },

  releasePublishArtifactsAction := PgpKeys.publishSigned.value,

  releaseCrossBuild := true,

  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseStepCommandAndRemaining("+publishLocalSigned"),
    setNextVersion,
    commitNextVersion
  ),

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
publish := {}
publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
releaseCrossBuild := false
