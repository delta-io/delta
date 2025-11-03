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

// scalastyle:off line.size.limit

import java.io.BufferedInputStream
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.util

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.utils.IOUtils

import scala.collection.mutable
import scala.sys.process._
import scala.util.Using

import sbt.internal.inc.Analysis
import sbtprotoc.ProtocPlugin.autoImport._

import xsbti.compile.CompileAnalysis

import Checkstyle._
import ShadedIcebergBuild._
import Mima._
import Unidoc._

// Scala versions
val scala213 = "2.13.16"
val all_scala_versions = Seq(scala213)

// Due to how publishArtifact is determined for javaOnlyReleaseSettings, incl. storage
// It was necessary to change default_scala_version to scala213 in build.sbt
// to build the project with Scala 2.13 only
// As a setting, it's possible to set it on command line easily
// sbt 'set default_scala_version := 2.13.16' [commands]
// FIXME Why not use scalaVersion?
val default_scala_version = settingKey[String]("Default Scala version")
Global / default_scala_version := scala213

// Cross-Spark-version building support
val sparkVersion = settingKey[String]("Spark version")
val internalModuleNames = settingKey[Set[String]]("Internal module artifact names to exclude from POM")
val crossSparkReleaseEnabled = sys.props.getOrElse("crossSparkRelease", "false").toBoolean

spark / sparkVersion := CrossSparkVersions.getSparkVersion()
sparkV1 / sparkVersion := CrossSparkVersions.getSparkVersion()
sparkV2 / sparkVersion := CrossSparkVersions.getSparkVersion()
connectCommon / sparkVersion := CrossSparkVersions.getSparkVersion()
connectClient / sparkVersion := CrossSparkVersions.getSparkVersion()
connectServer / sparkVersion := CrossSparkVersions.getSparkVersion()
sharing / sparkVersion := CrossSparkVersions.getSparkVersion()

// Dependent library versions
val defaultSparkVersion = SparkVersionSpec.LATEST_RELEASED.fullVersion
val hadoopVersion = "3.3.4"
val scalaTestVersion = "3.2.15"
val scalaTestVersionForConnectors = "3.0.8"
val parquet4sVersion = "1.9.4"

val protoVersion = "3.25.1"
val grpcVersion = "1.62.2"

scalaVersion := default_scala_version.value

// crossScalaVersions must be set to Nil on the root project
crossScalaVersions := Nil

lazy val javaVersion = sys.props.getOrElse("java.version", "Unknown")
lazy val javaVersionInt = javaVersion.split("\\.")(0).toInt

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := default_scala_version.value,
  crossScalaVersions := all_scala_versions,
  fork := true,
  scalacOptions ++= Seq("-Ywarn-unused:imports"),
  javacOptions ++= {
    if (javaVersion.startsWith("1.8")) {
      Seq.empty // `--release` is supported since JDK 9 and the minimum supported JDK is 8
    } else {
      Seq("--release", CrossSparkVersions.getSparkVersionSpec().targetJvm)
    }
  },

  // Make sure any tests in any project that uses Spark is configured for running well locally
  Test / javaOptions ++= Seq(
    "-Dspark.ui.enabled=false",
    "-Dspark.ui.showConsoleProgress=false",
    "-Dspark.databricks.delta.snapshotPartitions=2",
    "-Dspark.sql.shuffle.partitions=5",
    "-Ddelta.log.cacheSize=3",
    "-Dspark.databricks.delta.delta.log.cacheSize=3",
    "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
    "-Xmx1024m"
  ) ++ {
    if (javaVersionInt >= 17) {
      Seq(  // For Java 17 +
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
      )
    } else {
      Seq.empty
    }
  },

  testOptions += Tests.Argument("-oF"),

  // Unidoc settings: by default dont document any source file
  unidocSourceFilePatterns := Nil,
)

////////////////////////////
// START: Code Formatting //
////////////////////////////

/** Enforce java code style on compile. */
def javafmtCheckSettings(): Seq[Def.Setting[Task[CompileAnalysis]]] = Seq(
  (Compile / compile) := ((Compile / compile) dependsOn (Compile / javafmtCheckAll)).value
)

/** Enforce scala code style on compile. */
def scalafmtCheckSettings(): Seq[Def.Setting[Task[CompileAnalysis]]] = Seq(
  (Compile / compile) := ((Compile / compile) dependsOn (Compile / scalafmtCheckAll)).value,
)

// TODO: define fmtAll and fmtCheckAll tasks that run both scala and java fmts/checks

//////////////////////////
// END: Code Formatting //
//////////////////////////

/**
 * Note: we cannot access sparkVersion.value here, since that can only be used within a task or
 *       setting macro.
 */
def runTaskOnlyOnSparkMaster[T](
    task: sbt.TaskKey[T],
    taskName: String,
    projectName: String,
    emptyValue: => T): Def.Initialize[Task[T]] = {
  if (CrossSparkVersions.getSparkVersionSpec().isMaster) {
    Def.task(task.value)
  } else {
    Def.task {
      // scalastyle:off println
      println(s"Project $projectName: Skipping `$taskName` as Spark version " +
        s"${CrossSparkVersions.getSparkVersion()} does not equal ${SparkVersionSpec.MASTER.fullVersion}.")
      // scalastyle:on println
      emptyValue
    }
  }
}

lazy val connectCommon = (project in file("spark-connect/common"))
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "delta-connect-common",
    commonSettings,
    CrossSparkVersions.crossSparkSettings(default_scala_version, all_scala_versions, scala213),
    dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.2",
    releaseSettings,
    moduleName := CrossSparkVersions.moduleName(name.value, sparkVersion.value),
    Compile / compile := runTaskOnlyOnSparkMaster(
      task = Compile / compile,
      taskName = "compile",
      projectName = "delta-connect-common",
      emptyValue = Analysis.empty.asInstanceOf[CompileAnalysis]
    ).value,
    Test / test := runTaskOnlyOnSparkMaster(
      task = Test / test,
      taskName = "test",
      projectName = "delta-connect-common",
      emptyValue = ()).value,
    publish := runTaskOnlyOnSparkMaster(
      task = publish,
      taskName = "publish",
      projectName = "delta-connect-common",
      emptyValue = ()).value,
    libraryDependencies ++= Seq(
      "io.grpc" % "protoc-gen-grpc-java" % grpcVersion asProtocPlugin(),
      "io.grpc" % "grpc-protobuf" % grpcVersion,
      "io.grpc" % "grpc-stub" % grpcVersion,
      "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf",
      "javax.annotation" % "javax.annotation-api" % "1.3.2",

      "org.apache.spark" %% "spark-connect-common" % sparkVersion.value % "provided",
    ),
    PB.protocVersion := protoVersion,
    Compile / PB.targets := Seq(
      PB.gens.java -> (Compile / sourceManaged).value,
      PB.gens.plugin("grpc-java") -> (Compile / sourceManaged).value
    ),
  )

lazy val connectClient = (project in file("spark-connect/client"))
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .dependsOn(connectCommon % "compile->compile;test->test;provided->provided")
  .settings(
    name := "delta-connect-client",
    commonSettings,
    releaseSettings,
    moduleName := CrossSparkVersions.moduleName(name.value, sparkVersion.value),
    Compile / compile := runTaskOnlyOnSparkMaster(
      task = Compile / compile,
      taskName = "compile",
      projectName = "delta-connect-client",
      emptyValue = Analysis.empty.asInstanceOf[CompileAnalysis]
    ).value,
    Test / test := runTaskOnlyOnSparkMaster(
      task = Test / test,
      taskName = "test",
      projectName = "delta-connect-client",
      emptyValue = ()
    ).value,
    publish := runTaskOnlyOnSparkMaster(
      task = publish,
      taskName = "publish",
      projectName = "delta-connect-client",
      emptyValue = ()
    ).value,
    CrossSparkVersions.crossSparkSettings(default_scala_version, all_scala_versions, scala213),
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf",
      "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion.value % "provided",

      // Test deps
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion.value % "test" classifier "tests"
    ),
    (Test / javaOptions) += {
      // Create a (mini) Spark Distribution based on the server classpath.
      val serverClassPath = (connectServer / Compile / fullClasspath).value
      val distributionDir = crossTarget.value / "test-dist"
      if (!distributionDir.exists()) {
        val jarsDir = distributionDir / "jars"
        IO.createDirectory(jarsDir)
        // Create symlinks for all dependencies
        serverClassPath.distinct.foreach { entry =>
          val jarFile = entry.data.toPath
          val linkedJarFile = jarsDir / entry.data.getName
          Files.createSymbolicLink(linkedJarFile.toPath, jarFile)
        }
        // Create a symlink for the log4j properties
        val confDir = distributionDir / "conf"
        IO.createDirectory(confDir)
        val log4jProps = (spark / Test / resourceDirectory).value / "log4j2_spark_master.properties"
        val linkedLog4jProps = confDir / "log4j2.properties"
        Files.createSymbolicLink(linkedLog4jProps.toPath, log4jProps.toPath)
      }
      // Return the location of the distribution directory.
      "-Ddelta.spark.home=" + distributionDir
    },
    // Required for testing addFeatureSupport/dropFeatureSupport.
    Test / envVars += ("DELTA_TESTING", "1"),
  )

lazy val connectServer = (project in file("spark-connect/server"))
  .dependsOn(connectCommon % "compile->compile;test->test;provided->provided")
  .dependsOn(spark % "compile->compile;test->test;provided->provided")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "delta-connect-server",
    commonSettings,
    releaseSettings,
    moduleName := CrossSparkVersions.moduleName(name.value, sparkVersion.value),
    Compile / compile := runTaskOnlyOnSparkMaster(
      task = Compile / compile,
      taskName = "compile",
      projectName = "delta-connect-server",
      emptyValue = Analysis.empty.asInstanceOf[CompileAnalysis]
    ).value,
    Test / test := runTaskOnlyOnSparkMaster(
      task = Test / test,
      taskName = "test",
      projectName = "delta-connect-server",
      emptyValue = ()
    ).value,
    publish := runTaskOnlyOnSparkMaster(
      task = publish,
      taskName = "publish",
      projectName = "delta-connect-server",
      emptyValue = ()
    ).value,
    CrossSparkVersions.crossSparkSettings(default_scala_version, all_scala_versions, scala213),
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf",

      "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-connect" % sparkVersion.value % "provided",

      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-hive" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-connect" % sparkVersion.value % "test" classifier "tests",
    ),
    excludeDependencies ++= Seq(
      // Exclude connect common because a properly shaded version of it is included in the
      // spark-connect jar. Including it causes classpath problems.
      ExclusionRule("org.apache.spark", "spark-connect-common_2.13"),
      // Exclude connect shims because we have spark-core on the classpath. The shims are only
      // needed for the client. Including it causes classpath problems.
      ExclusionRule("org.apache.spark", "spark-connect-shims_2.13")
    ),
    // Required for testing addFeatureSupport/dropFeatureSupport.
    Test / envVars += ("DELTA_TESTING", "1"),
  )

lazy val deltaSuiteGenerator = (project in file("spark/delta-suite-generator"))
  .disablePlugins(ScalafmtPlugin)
  .settings (
    name := "delta-suite-generator",
    commonSettings,
    scalaStyleSettings,
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
      "org.scalameta" %% "scalameta" % "4.13.5",
      "org.scalameta" %% "scalafmt-core" % "3.9.6",
      "commons-cli" % "commons-cli" % "1.9.0",
      "commons-codec" % "commons-codec" % "1.17.2",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    ),
    Compile / mainClass := Some("io.delta.suitegenerator.ModularSuiteGenerator"),
    Test / baseDirectory := (ThisBuild / baseDirectory).value,
  )

// ============================================================
// Spark Module 1: sparkV1 (prod code only, no tests)
// ============================================================
lazy val sparkV1 = (project in file("spark"))
  .dependsOn(storage)
  .enablePlugins(Antlr4Plugin)
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings (
    name := "delta-spark-v1",
    commonSettings,
    scalaStyleSettings,
    skipReleaseSettings, // Internal module - not published to Maven
    CrossSparkVersions.crossSparkSettings(default_scala_version, all_scala_versions, scala213),
    moduleName := CrossSparkVersions.moduleName(name.value, sparkVersion.value),

    // Export as JAR instead of classes directory. This prevents dependent projects
    // (e.g., connectServer) from seeing multiple 'classes' directories with the same
    // name in their classpath, which would cause FileAlreadyExistsException.
    exportJars := true,

    // Tests are compiled in the final 'spark' module to avoid circular dependencies
    Test / sources := Seq.empty,
    Test / resources := Seq.empty,

    libraryDependencies ++= Seq(
      // Adding test classifier seems to break transitive resolution of the core dependencies
      "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",
      // For DynamoDBCommitStore
      "com.amazonaws" % "aws-java-sdk" % "1.12.262" % "provided",

      // Test deps
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test",
      "junit" % "junit" % "4.13.2" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-hive" % sparkVersion.value % "test" classifier "tests",
      "org.mockito" % "mockito-inline" % "4.11.0" % "test",
    ),
    Compile / packageBin / mappings := (Compile / packageBin / mappings).value ++
        listPythonFiles(baseDirectory.value.getParentFile / "python"),
    Antlr4 / antlr4PackageName := Some("io.delta.sql.parser"),
    Antlr4 / antlr4GenListener := true,
    Antlr4 / antlr4GenVisitor := true,

    // Introduced in https://github.com/delta-io/delta/commit/d2990624d34b6b86fa5cf230e00a89b095fde254
    //
    // Hack to avoid errors related to missing repo-root/target/scala-2.13/classes/
    // In multi-module sbt projects, some dependencies may attempt to locate this directory
    // at the repository root, causing build failures if it doesn't exist.
    createTargetClassesDir := {
      val dir = baseDirectory.value.getParentFile / "target" / "scala-2.13" / "classes"
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
  )

// ============================================================
// Spark Module 2: sparkV1Filtered (v1 without DeltaLog for v2 dependency)
// This filtered version of sparkV1 is needed because sparkV2 (kernel-spark) depends on some
// V1 classes for utilities and common functionality, but must NOT have access to DeltaLog,
// Snapshot, OptimisticTransaction, or actions that belongs to core V1 delta libraries.
// We should use Kernel as the Delta implementation.
// ============================================================
lazy val sparkV1Filtered = (project in file("spark-v1-filtered"))
  .dependsOn(sparkV1)
  .dependsOn(storage)
  .settings(
    name := "delta-spark-v1-filtered",
    commonSettings,
    skipReleaseSettings, // Internal module - not published to Maven
    exportJars := true,  // Export as JAR to avoid classpath conflicts

    // No source code - just repackage sparkV1 without DeltaLog classes
    Compile / sources := Seq.empty,
    Test / sources := Seq.empty,

    // Repackage sparkV1 jar but exclude DeltaLog and related classes
    Compile / packageBin / mappings := {
      val v1Mappings = (sparkV1 / Compile / packageBin / mappings).value

      // Filter out DeltaLog, Snapshot, OptimisticTransaction, and actions.scala classes
      v1Mappings.filterNot { case (file, path) =>
        path.contains("org/apache/spark/sql/delta/DeltaLog") ||
        path.contains("org/apache/spark/sql/delta/Snapshot") ||
        path.contains("org/apache/spark/sql/delta/OptimisticTransaction") ||
        path.contains("org/apache/spark/sql/delta/actions/actions")
      }
    },
  )

// ============================================================
// Spark Module 3: sparkV2 (kernel-spark based, depends on v1-filtered)
// ============================================================
lazy val sparkV2 = (project in file("kernel-spark"))
  .dependsOn(sparkV1Filtered)
  .dependsOn(kernelApi)
  .dependsOn(kernelDefaults)
  .dependsOn(goldenTables % "test")
  .settings(
    name := "delta-spark-v2",
    commonSettings,
    javafmtCheckSettings,
    skipReleaseSettings, // Internal module - not published to Maven
    exportJars := true,  // Export as JAR to avoid classpath conflicts

    Test / javaOptions ++= Seq("-ea"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",

      // Test dependencies
      "org.junit.jupiter" % "junit-jupiter-api" % "5.8.2" % "test",
      "org.junit.jupiter" % "junit-jupiter-engine" % "5.8.2" % "test",
      "org.junit.jupiter" % "junit-jupiter-params" % "5.8.2" % "test",
      "net.aichler" % "jupiter-interface" % "0.11.1" % "test",
      // Spark test classes for Scala/Java test utilities
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
      // ScalaTest for test utilities (needed by Spark test classes)
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),
    Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
  )


// ============================================================
// Spark Module 4: delta-spark (final published module - unified v1+v2)
// ============================================================
lazy val spark = (project in file("spark-unified"))
  .dependsOn(sparkV1)
  .dependsOn(sparkV2)
  .dependsOn(storage)
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings (
    name := "delta-spark",
    commonSettings,
    scalaStyleSettings,
    sparkMimaSettings,
    releaseSettings, // Published to Maven as delta-spark.jar

    // Set Test baseDirectory before crossSparkSettings() so it uses the correct directory
    Test / baseDirectory := (sparkV1 / baseDirectory).value,

    // Test sources from spark/ directory (sparkV1's directory)
    // MUST be set BEFORE crossSparkSettings() to avoid overwriting version-specific directories
    Test / unmanagedSourceDirectories := {
      val sparkDir = (sparkV1 / baseDirectory).value
      Seq(
        sparkDir / "src" / "test" / "scala",
        sparkDir / "src" / "test" / "java"
      )
    },
    Test / unmanagedResourceDirectories := Seq(
      (sparkV1 / baseDirectory).value / "src" / "test" / "resources"
    ),

    crossSparkSettings(),

    // MiMa should use the generated JAR (not classDirectory) because we merge classes at package time
    mimaCurrentClassfiles := (Compile / packageBin).value,

    // Export as JAR to dependent projects (e.g., connectServer, connectClient).
    // This prevents classpath conflicts from internal module 'classes' directories.
    exportJars := true,

    // Internal module artifact names to exclude from published POM
    internalModuleNames := Set("delta-spark-v1", "delta-spark-v1-shaded", "delta-spark-v2"),

    // Merge classes from internal modules (v1, v2) into final JAR
    // kernel modules are kept as separate JARs and listed as dependencies in POM
    Compile / packageBin / mappings ++= {
      val log = streams.value.log

      // Collect mappings from internal modules
      val v1Mappings = (sparkV1 / Compile / packageBin / mappings).value
      val v2Mappings = (sparkV2 / Compile / packageBin / mappings).value

      // Include Python files (from spark/ directory)
      val pythonMappings = listPythonFiles(baseDirectory.value.getParentFile / "python")

      // Combine all mappings
      val allMappings = v1Mappings ++ v2Mappings ++ pythonMappings

      // Detect duplicate class files
      val classFiles = allMappings.filter(_._2.endsWith(".class"))
      val duplicates = classFiles.groupBy(_._2).filter(_._2.size > 1)

      if (duplicates.nonEmpty) {
        log.error(s"Found ${duplicates.size} duplicate class(es) in packageBin mappings:")
        duplicates.foreach { case (className, entries) =>
          log.error(s"  - $className:")
          entries.foreach { case (file, path) => log.error(s"      from: $file") }
        }
        sys.error("Duplicate classes found. This indicates overlapping code between sparkV1, sparkV2, and storage modules.")
      }

      allMappings.distinct
    },

    // Exclude internal modules from published POM
    pomPostProcess := { node =>
      val internalModules = internalModuleNames.value
      import scala.xml._
      import scala.xml.transform._
      new RuleTransformer(new RewriteRule {
        override def transform(n: Node): Seq[Node] = n match {
          case e: Elem if e.label == "dependency" =>
            val artifactId = (e \ "artifactId").text
            if (internalModules.contains(artifactId)) Seq.empty else Seq(n)
          case _ => Seq(n)
        }
      }).transform(node).head
    },

    pomIncludeRepository := { _ => false },

    // Filter internal modules from project dependencies
    // This works together with pomPostProcess to ensure internal modules
    // (sparkV1, sparkV2, sparkV1Filtered) are not listed as dependencies in POM
    projectDependencies := {
      val internalModules = internalModuleNames.value
      projectDependencies.value.filterNot(dep => internalModules.contains(dep.name))
    },

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",
      "com.amazonaws" % "aws-java-sdk" % "1.12.262" % "provided",

      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test",
      "junit" % "junit" % "4.13.2" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-hive" % sparkVersion.value % "test" classifier "tests",
      "org.mockito" % "mockito-inline" % "4.11.0" % "test",
    ),

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
      "-Dspark.databricks.delta.delta.log.cacheSize=3",
      "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
      "-Xmx1024m"
    ),

    // Required for testing table features see https://github.com/delta-io/delta/issues/1602
    Test / envVars += ("DELTA_TESTING", "1"),

    TestParallelization.settings,
  )
  .configureUnidoc(
    generatedJavaDoc = CrossSparkVersions.getSparkVersionSpec().isLatestReleased,
    generateScalaDoc = CrossSparkVersions.getSparkVersionSpec().isLatestReleased,
    // spark-connect has classes with the same name as spark-core, this causes compilation issues
    // with unidoc since it concatenates the classpaths from all modules
    // ==> thus we exclude such sources
    // (mostly) relevant github issue: https://github.com/sbt/sbt-unidoc/issues/77
    classPathToSkip = "spark-connect"
  )

lazy val contribs = (project in file("contribs"))
  .dependsOn(spark % "compile->compile;test->test;provided->provided")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings (
    name := "delta-contribs",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    moduleName := CrossSparkVersions.moduleName(name.value, (spark / sparkVersion).value),
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
      "-Dspark.databricks.delta.delta.log.cacheSize=3",
      "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
      "-Xmx1024m"
    ),

    // Introduced in https://github.com/delta-io/delta/commit/d2990624d34b6b86fa5cf230e00a89b095fde254
    //
    // Hack to avoid errors related to missing repo-root/target/scala-2.13/classes/
    // In multi-module sbt projects, some dependencies may attempt to locate this directory
    // at the repository root, causing build failures if it doesn't exist.
    createTargetClassesDir := {
      val dir = baseDirectory.value.getParentFile / "target" / "scala-2.13" / "classes"
      Files.createDirectories(dir.toPath)
    },
    Compile / compile := ((Compile / compile) dependsOn createTargetClassesDir).value
  ).configureUnidoc()

lazy val sharing = (project in file("sharing"))
  .dependsOn(spark % "compile->compile;test->test;provided->provided")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "delta-sharing-spark",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    CrossSparkVersions.crossSparkSettings(default_scala_version, all_scala_versions, scala213),
    moduleName := CrossSparkVersions.moduleName(name.value, sparkVersion.value),
    Test / javaOptions ++= Seq("-ea"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",

      "io.delta" %% "delta-sharing-client" % "1.2.4",

      // Test deps
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test",
      "junit" % "junit" % "4.13.2" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-hive" % sparkVersion.value % "test" classifier "tests",
    )
  ).configureUnidoc()

lazy val kernelApi = (project in file("kernel/kernel-api"))
  .enablePlugins(ScalafmtPlugin)
  .settings(
    name := "delta-kernel-api",
    commonSettings,
    scalaStyleSettings,
    javaOnlyReleaseSettings,
    javafmtCheckSettings,
    scalafmtCheckSettings,

    // Use unique classDirectory name to avoid conflicts in connectClient test setup
    // This allows connectClient to create symlinks without FileAlreadyExistsException
    Compile / classDirectory := target.value / "scala-2.13" / "kernel-api-classes",

    Test / javaOptions ++= Seq("-ea"),
    libraryDependencies ++= Seq(
      "org.roaringbitmap" % "RoaringBitmap" % "0.9.25",
      "org.slf4j" % "slf4j-api" % "1.7.36",

      "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.13.5",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.13.5",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.13.5",

      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "junit" % "junit" % "4.13.2" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.slf4j" % "slf4j-log4j12" % "1.7.36" % "test",
      "org.assertj" % "assertj-core" % "3.26.3" % "test",
      // JMH dependencies allow writing micro-benchmarks for testing performance of components.
      // JMH has framework to define benchmarks and takes care of many common functionalities
      // such as warm runs, cold runs, defining benchmark parameter variables etc.
      "org.openjdk.jmh" % "jmh-core" % "1.37" % "test",
      "org.openjdk.jmh" % "jmh-generator-annprocess" % "1.37" % "test"
    ),
    // Shade jackson libraries so that connector developers don't have to worry
    // about jackson version conflicts.
    Compile / packageBin := assembly.value,
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    assembly / logLevel := Level.Info,
    assembly / test := {},
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      val allowedPrefixes = Set("META_INF", "io", "jackson")
      cp.filter { f =>
        !allowedPrefixes.exists(prefix => f.data.getName.startsWith(prefix))
      }
    },
     assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.fasterxml.jackson.**" -> "io.delta.kernel.shaded.com.fasterxml.jackson.@1").inAll
    ),
    assembly / assemblyMergeStrategy := {
      // Discard `module-info.class` to fix the `different file contents found` error.
      // TODO Upgrade SBT to 1.5 which will do this automatically
      case "module-info.class" => MergeStrategy.discard
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    // Generate the package object to provide the version information in runtime.
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "io" / "delta" / "kernel" / "Meta.java"
      IO.write(file,
        s"""/*
           | * Copyright (2024) The Delta Lake Project Authors.
           | *
           | * Licensed under the Apache License, Version 2.0 (the "License");
           | * you may not use this file except in compliance with the License.
           | * You may obtain a copy of the License at
           | *
           | * http://www.apache.org/licenses/LICENSE-2.0
           | *
           | * Unless required by applicable law or agreed to in writing, software
           | * distributed under the License is distributed on an "AS IS" BASIS,
           | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
           | * See the License for the specific language governing permissions and
           | * limitations under the License.
           | */
           |package io.delta.kernel;
           |
           |public final class Meta {
           |    public static final String KERNEL_VERSION = "${version.value}";
           |}
           |""".stripMargin)
      Seq(file)
    },
    MultiShardMultiJVMTestParallelization.settings,
    javaCheckstyleSettings("dev/kernel-checkstyle.xml"),
    // Unidoc settings
    unidocSourceFilePatterns := Seq(SourceFilePattern("io/delta/kernel/")),
  ).configureUnidoc(docTitle = "Delta Kernel")

lazy val kernelDefaults = (project in file("kernel/kernel-defaults"))
  .enablePlugins(ScalafmtPlugin)
  .dependsOn(kernelApi)
  .dependsOn(kernelApi % "test->test")
  .dependsOn(storage)
  .dependsOn(storage % "test->test") // Required for InMemoryCommitCoordinator for tests
  .dependsOn(goldenTables % "test")
  .settings(
    name := "delta-kernel-defaults",
    commonSettings,
    scalaStyleSettings,
    javaOnlyReleaseSettings,
    javafmtCheckSettings,
    scalafmtCheckSettings,

    // Use unique classDirectory name to avoid conflicts in connectClient test setup
    // This allows connectClient to create symlinks without FileAlreadyExistsException
    Compile / classDirectory := target.value / "scala-2.13" / "kernel-defaults-classes",

    Test / javaOptions ++= Seq("-ea"),
    // This allows generating tables with unsupported test table features in delta-spark
    Test / envVars += ("DELTA_TESTING", "1"),
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.13.5",
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3",

      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "junit" % "junit" % "4.13.2" % "test",
      "commons-io" % "commons-io" % "2.8.0" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.slf4j" % "slf4j-log4j12" % "1.7.36" % "test",
      // JMH dependencies allow writing micro-benchmarks for testing performance of components.
      // JMH has framework to define benchmarks and takes care of many common functionalities
      // such as warm runs, cold runs, defining benchmark parameter variables etc.
      "org.openjdk.jmh" % "jmh-core" % "1.37" % "test",
      "org.openjdk.jmh" % "jmh-generator-annprocess" % "1.37" % "test",
      "io.delta" %% "delta-spark" % "3.3.2" % "test",

      "org.apache.spark" %% "spark-hive" % defaultSparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % defaultSparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % defaultSparkVersion % "test" classifier "tests",
    ),
    MultiShardMultiJVMTestParallelization.settings,
    javaCheckstyleSettings("dev/kernel-checkstyle.xml"),
      // Unidoc settings
    unidocSourceFilePatterns += SourceFilePattern("io/delta/kernel/"),
  ).configureUnidoc(docTitle = "Delta Kernel Defaults")

lazy val unity = (project in file("unity"))
  .enablePlugins(ScalafmtPlugin)
  .dependsOn(kernelApi % "compile->compile;test->test")
  .dependsOn(kernelDefaults % "test->test")
  .dependsOn(storage)
  .settings (
    name := "delta-unity",
    commonSettings,
    javaOnlyReleaseSettings,
    javafmtCheckSettings,
    javaCheckstyleSettings("dev/kernel-checkstyle.xml"),
    scalaStyleSettings,
    scalafmtCheckSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    ),
    unidocSourceFilePatterns += SourceFilePattern("src/main/java/io/delta/unity/"),
  ).configureUnidoc()

// TODO javastyle tests
// TODO unidoc
// TODO(scott): figure out a better way to include tests in this project
lazy val storage = (project in file("storage"))
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings (
    name := "delta-storage",
    commonSettings,
    exportJars := true,
    javaOnlyReleaseSettings,
    libraryDependencies ++= Seq(
      // User can provide any 2.x or 3.x version. We don't use any new fancy APIs. Watch out for
      // versions with known vulnerabilities.
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",

      // Note that the org.apache.hadoop.fs.s3a.Listing::createFileStatusListingIterator 3.3.1 API
      // is not compatible with 3.3.2.
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "provided",

      // Test Deps
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    ),

    // Unidoc settings
    unidocSourceFilePatterns += SourceFilePattern("/LogStore.java", "/CloseableIterator.java"),
  ).configureUnidoc()

lazy val storageS3DynamoDB = (project in file("storage-s3-dynamodb"))
  .dependsOn(storage % "compile->compile;test->test;provided->provided")
  .dependsOn(spark % "test->test")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings (
    name := "delta-storage-s3-dynamodb",
    commonSettings,
    javaOnlyReleaseSettings,

    // uncomment only when testing FailingS3DynamoDBLogStore. this will include test sources in
    // a separate test jar.
    // Test / publishArtifact := true,

    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.12.262" % "provided",

      // Test Deps
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "test", // RemoteFileChangedException
    )
  ).configureUnidoc()

val icebergSparkRuntimeArtifactName = {
 val (expMaj, expMin, _) = getMajorMinorPatch(defaultSparkVersion)
 s"iceberg-spark-runtime-$expMaj.$expMin"
}

lazy val testDeltaIcebergJar = (project in file("testDeltaIcebergJar"))
  // delta-iceberg depends on delta-spark! So, we need to include it during our test.
  .dependsOn(spark % "test")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "test-delta-iceberg-jar",
    commonSettings,
    skipReleaseSettings,
    exportJars := true,
    Compile / unmanagedJars += (iceberg / assembly).value,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.apache.spark" %% "spark-core" % defaultSparkVersion % "test"
    )
  )

val deltaIcebergSparkIncludePrefixes = Seq(
  // We want everything from this package
  "org/apache/spark/sql/delta/icebergShaded",

  // We only want the files in this project from this package. e.g. we want to exclude
  // org/apache/spark/sql/delta/commands/convert/ConvertTargetFile.class (from delta-spark project).
  "org/apache/spark/sql/delta/commands/convert/IcebergFileManifest",
  "org/apache/spark/sql/delta/commands/convert/IcebergSchemaUtils",
  "org/apache/spark/sql/delta/commands/convert/IcebergTable"
)

// Build using: build/sbt clean icebergShaded/compile iceberg/compile
// It will fail the first time, just re-run it.
// scalastyle:off println
lazy val iceberg = (project in file("iceberg"))
  .dependsOn(spark % "compile->compile;test->test;provided->provided")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings (
    name := "delta-iceberg",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    moduleName := CrossSparkVersions.moduleName(name.value, (spark / sparkVersion).value),
    libraryDependencies ++= Seq(
      // Fix Iceberg's legacy java.lang.NoClassDefFoundError: scala/jdk/CollectionConverters$ error
      // due to legacy scala.
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.1",
      "org.apache.iceberg" %% icebergSparkRuntimeArtifactName % "1.4.0" % "provided",
      "com.github.ben-manes.caffeine" % "caffeine" % "2.9.3"
    ),
    Compile / unmanagedJars += (icebergShaded / assembly).value,
    // Generate the assembly JAR as the package JAR
    Compile / packageBin := assembly.value,
    assembly / assemblyJarName := {
      s"${moduleName.value}_${scalaBinaryVersion.value}-${version.value}.jar"
    },
    assembly / logLevel := Level.Info,
    assembly / test := {},
    assembly / assemblyExcludedJars := {
      // Note: the input here is only `libraryDependencies` jars, not `.dependsOn(_)` jars.
      val allowedJars = Seq(
        s"iceberg-shaded_${scalaBinaryVersion.value}-${version.value}.jar",
        s"scala-library-${scala213}.jar",
        s"scala-collection-compat_${scalaBinaryVersion.value}-2.1.1.jar",
        "caffeine-2.9.3.jar",
        // Note: We are excluding
        // - antlr4-runtime-4.9.3.jar
        // - checker-qual-3.19.0.jar
        // - error_prone_annotations-2.10.0.jar
      )
      val cp = (assembly / fullClasspath).value

      // Return `true` when we want the jar `f` to be excluded from the assembly jar
      cp.filter { f =>
        val doExclude = !allowedJars.contains(f.data.getName)
        println(s"Excluding jar: ${f.data.getName} ? $doExclude")
        doExclude
      }
    },
    assembly / assemblyMergeStrategy := {
      // Project iceberg `dependsOn` spark and accidentally brings in it, along with its
      // compile-time dependencies (like delta-storage). We want these excluded from the
      // delta-iceberg jar.
      case PathList("io", "delta", xs @ _*) =>
        // - delta-storage will bring in classes: io/delta/storage
        // - delta-spark will bring in classes: io/delta/exceptions/, io/delta/implicits,
        //   io/delta/package, io/delta/sql, io/delta/tables,
        MergeStrategy.discard
      case PathList("com", "databricks", xs @ _*) =>
        // delta-spark will bring in com/databricks/spark/util
        MergeStrategy.discard
      case PathList("org", "apache", "spark", xs @ _*)
        if !deltaIcebergSparkIncludePrefixes.exists { prefix =>
          s"org/apache/spark/${xs.mkString("/")}".startsWith(prefix) } =>
        MergeStrategy.discard
      case PathList("scoverage", xs @ _*) =>
        MergeStrategy.discard
      case x =>
        (assembly / assemblyMergeStrategy).value(x)
    },
    assemblyPackageScala / assembleArtifact := false
  )
// scalastyle:on println

val icebergShadedVersion = "1.10.0"
lazy val icebergShaded = (project in file("icebergShaded"))
  .dependsOn(spark % "provided")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings (
    name := "iceberg-shaded",
    commonSettings,
    skipReleaseSettings,
    // must exclude all dependencies from Iceberg that delta-spark includes
    libraryDependencies ++= Seq(
      // Fix Iceberg's legacy java.lang.NoClassDefFoundError: scala/jdk/CollectionConverters$ error
      // due to legacy scala.
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.1" % "provided",
      "org.apache.iceberg" % "iceberg-core" % icebergShadedVersion excludeAll (
        icebergExclusionRules: _*
      ),
      "org.apache.iceberg" % "iceberg-hive-metastore" % icebergShadedVersion excludeAll (
        icebergExclusionRules: _*
      ),
      // the hadoop client and hive metastore versions come from this file in the
      // iceberg repo of icebergShadedVersion: iceberg/gradle/libs.versions.toml
      "org.apache.hadoop" % "hadoop-client" % "2.7.3" % "provided" excludeAll (
        hadoopClientExclusionRules: _*
      ),
      "org.apache.hive" % "hive-metastore" % "2.3.8" % "provided" excludeAll (
        hiveMetastoreExclusionRules: _*
      )
    ),
    // Generated shaded Iceberg JARs
    Compile / packageBin := assembly.value,
    assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar",
    assembly / logLevel := Level.Info,
    assembly / test := {},
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("org.apache.iceberg.**" -> "shadedForDelta.@0").inAll
    ),
    assembly / assemblyExcludedJars := {
      val cp = (fullClasspath in assembly).value
      cp.filter { jar =>
        val doExclude = jar.data.getName.contains("jackson-annotations") ||
          jar.data.getName.contains("RoaringBitmap")
        doExclude
      }
    },
    // all following clases have Delta customized implementation under icebergShaded/src and thus
    // require them to be 'first' to replace the class from iceberg jar
    assembly / assemblyMergeStrategy := updateMergeStrategy((assembly / assemblyMergeStrategy).value),
    assemblyPackageScala / assembleArtifact := false,
  )

lazy val hudi = (project in file("hudi"))
  .dependsOn(spark % "compile->compile;test->test;provided->provided")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings (
    name := "delta-hudi",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    libraryDependencies ++= Seq(
      "org.apache.hudi" % "hudi-java-client" % "0.15.0" % "compile" excludeAll(
        ExclusionRule(organization = "org.apache.hadoop"),
        ExclusionRule(organization = "org.apache.zookeeper"),
      ),
      "org.apache.spark" %% "spark-avro" % defaultSparkVersion % "test" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
      "org.apache.parquet" % "parquet-avro" % "1.12.3" % "compile"
    ),
    assembly / assemblyJarName := s"${name.value}-assembly_${scalaBinaryVersion.value}-${version.value}.jar",
    assembly / logLevel := Level.Info,
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      // Project hudi `dependsOn` spark and accidentally brings in it, along with its
      // compile-time dependencies (like delta-storage). We want these excluded from the
      // delta-hudi jar.
      case PathList("io", "delta", xs @ _*) =>
        // - delta-storage will bring in classes: io/delta/storage
        // - delta-spark will bring in classes: io/delta/exceptions/, io/delta/implicits,
        //   io/delta/package, io/delta/sql, io/delta/tables,
        MergeStrategy.discard
      case PathList("com", "databricks", xs @ _*) =>
        // delta-spark will bring in com/databricks/spark/util
        MergeStrategy.discard
      case PathList("org", "apache", "spark", "sql", "delta", "hudi", xs @ _*) =>
        MergeStrategy.first
      case PathList("org", "apache", "spark", xs @ _*) =>
        MergeStrategy.discard
      // Discard `module-info.class` to fix the `different file contents found` error.
      // TODO Upgrade SBT to 1.5 which will do this automatically
      case "module-info.class" => MergeStrategy.discard
      // Discard unused `parquet.thrift` so that we don't conflict the file used by the user
      case "parquet.thrift" => MergeStrategy.discard
      // Hudi metadata writer requires this service file to be present on the classpath
      case "META-INF/services/org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceFactory" => MergeStrategy.first
      // Discard the jackson service configs that we don't need. These files are not shaded so
      // adding them may conflict with other jackson version used by the user.
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.discard
      case x =>
        MergeStrategy.first
    },
    // Make the 'compile' invoke the 'assembly' task to generate the uber jar.
    Compile / packageBin := assembly.value
  )

/**
 * We want to publish the `standalone` project's shaded JAR (created from the
 * build/sbt standalone/assembly command).
 *
 * However, build/sbt standalone/publish and build/sbt standalone/publishLocal will use the
 * non-shaded JAR from the build/sbt standalone/package command.
 *
 * So, we create an impostor, cosmetic project used only for publishing.
 *
 * build/sbt standalone/package
 * - creates connectors/standalone/target/scala-2.12/delta-standalone-original-shaded_2.12-0.2.1-SNAPSHOT.jar
 *   (this is the shaded JAR we want)
 *
 * build/sbt standaloneCosmetic/publishM2
 * - packages the shaded JAR (above) and then produces:
 * -- .m2/repository/io/delta/delta-standalone_2.12/0.2.1-SNAPSHOT/delta-standalone_2.12-0.2.1-SNAPSHOT.pom
 * -- .m2/repository/io/delta/delta-standalone_2.12/0.2.1-SNAPSHOT/delta-standalone_2.12-0.2.1-SNAPSHOT.jar
 * -- .m2/repository/io/delta/delta-standalone_2.12/0.2.1-SNAPSHOT/delta-standalone_2.12-0.2.1-SNAPSHOT-sources.jar
 * -- .m2/repository/io/delta/delta-standalone_2.12/0.2.1-SNAPSHOT/delta-standalone_2.12-0.2.1-SNAPSHOT-javadoc.jar
 */
lazy val standaloneCosmetic = project
  .dependsOn(storage) // this doesn't impact the output artifact (jar), only the pom.xml dependencies
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "delta-standalone",
    commonSettings,
    releaseSettings,
    exportJars := true,
    Compile / packageBin := (standaloneParquet / assembly).value,
    Compile / packageSrc := (standalone / Compile / packageSrc).value,
    libraryDependencies ++= scalaCollectionPar(scalaVersion.value) ++ Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3" % "provided",
      // parquet4s-core dependencies that are not shaded are added with compile scope.
      "com.chuusai" %% "shapeless" % "2.3.4",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.3"
    )
  )

lazy val testStandaloneCosmetic = (project in file("connectors/testStandaloneCosmetic"))
  .dependsOn(standaloneCosmetic)
  .dependsOn(goldenTables % "test")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "test-standalone-cosmetic",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test",
    )
  )

/**
 * A test project to verify `ParquetSchemaConverter` APIs are working after the user provides
 * `parquet-hadoop`. We use a separate project because we want to test whether Delta Standlone APIs
 * except `ParquetSchemaConverter` are working without `parquet-hadoop` in testStandaloneCosmetic`.
 */
lazy val testParquetUtilsWithStandaloneCosmetic = project.dependsOn(standaloneCosmetic)
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "test-parquet-utils-with-standalone-cosmetic",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3" % "provided",
      "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test",
    )
  )

def scalaCollectionPar(version: String) = version match {
  case v if v.startsWith("2.13.") =>
    Seq("org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4")
  case _ => Seq()
}

/**
 * The public API ParquetSchemaConverter exposes Parquet classes in its methods so we cannot apply
 * shading rules on it. However, sbt-assembly doesn't allow excluding a single file. Hence, we
 * create a separate project to skip the shading.
 */
lazy val standaloneParquet = (project in file("connectors/standalone-parquet"))
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .dependsOn(standaloneWithoutParquetUtils)
  .settings(
    name := "delta-standalone-parquet",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3" % "provided",
      "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test"
    ),
    assemblyPackageScala / assembleArtifact := false
  )

/** A dummy project to allow `standaloneParquet` depending on the shaded standalone jar. */
lazy val standaloneWithoutParquetUtils = project
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "delta-standalone-without-parquet-utils",
    commonSettings,
    skipReleaseSettings,
    exportJars := true,
    Compile / packageBin := (standalone / assembly).value
  )

// TODO scalastyle settings
lazy val standalone = (project in file("connectors/standalone"))
  .dependsOn(storage % "compile->compile;provided->provided")
  .dependsOn(goldenTables % "test")
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "delta-standalone-original",
    commonSettings,
    skipReleaseSettings,
    standaloneMimaSettings,
    // When updating any dependency here, we should also review `pomPostProcess` in project
    // `standaloneCosmetic` and update it accordingly.
    libraryDependencies ++= scalaCollectionPar(scalaVersion.value) ++ Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "com.github.mjakubowski84" %% "parquet4s-core" % parquet4sVersion excludeAll (
        ExclusionRule("org.slf4j", "slf4j-api")
        ),
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.3",
      "org.json4s" %% "json4s-jackson" % "3.7.0-M11" excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ),
      "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test",
    ),
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "io" / "delta" / "standalone" / "package.scala"
      IO.write(file,
        s"""package io.delta
           |
           |package object standalone {
           |  val VERSION = "${version.value}"
           |  val NAME = "Delta Standalone"
           |}
           |""".stripMargin)
      Seq(file)
    },

    /**
     * Standalone packaged (unshaded) jar.
     *
     * Build with `build/sbt standalone/package` command.
     * e.g. connectors/standalone/target/scala-2.12/delta-standalone-original-unshaded_2.12-0.2.1-SNAPSHOT.jar
     */
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      artifact.name + "-unshaded" + "_" + sv.binary + "-" + module.revision  + "." + artifact.extension
    },

    /**
     * Standalone assembly (shaded) jar. This is what we want to release.
     *
     * Build with `build/sbt standalone/assembly` command.
     * e.g. connectors/standalone/target/scala-2.12/delta-standalone-original-shaded_2.12-0.2.1-SNAPSHOT.jar
     */
    assembly / logLevel := Level.Info,
    assembly / test := {},
    assembly / assemblyJarName := s"${name.value}-shaded_${scalaBinaryVersion.value}-${version.value}.jar",
    // We exclude jars first, and then we shade what is remaining. Note: the input here is only
    // `libraryDependencies` jars, not `.dependsOn(_)` jars.
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      val allowedPrefixes = Set("META_INF", "io", "json4s", "jackson", "paranamer",
        "parquet4s", "parquet-", "audience-annotations", "commons-pool")
      cp.filter { f =>
        !allowedPrefixes.exists(prefix => f.data.getName.startsWith(prefix))
      }
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.fasterxml.jackson.**" -> "shadedelta.@0").inAll,
      ShadeRule.rename("com.thoughtworks.paranamer.**" -> "shadedelta.@0").inAll,
      ShadeRule.rename("org.json4s.**" -> "shadedelta.@0").inAll,
      ShadeRule.rename("com.github.mjakubowski84.parquet4s.**" -> "shadedelta.@0").inAll,
      ShadeRule.rename("org.apache.commons.pool.**" -> "shadedelta.@0").inAll,
      ShadeRule.rename("org.apache.parquet.**" -> "shadedelta.@0").inAll,
      ShadeRule.rename("shaded.parquet.**" -> "shadedelta.@0").inAll,
      ShadeRule.rename("org.apache.yetus.audience.**" -> "shadedelta.@0").inAll
    ),
    assembly / assemblyMergeStrategy := {
      // Discard `module-info.class` to fix the `different file contents found` error.
      // TODO Upgrade SBT to 1.5 which will do this automatically
      case "module-info.class" => MergeStrategy.discard
      // Discard unused `parquet.thrift` so that we don't conflict the file used by the user
      case "parquet.thrift" => MergeStrategy.discard
      // Discard the jackson service configs that we don't need. These files are not shaded so
      // adding them may conflict with other jackson version used by the user.
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.discard
      // This project `.dependsOn` delta-storage, and its classes will be included by default
      // in this assembly jar. Manually discard them since it is already a compile-time dependency.
      case PathList("io", "delta", "storage", xs @ _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / artifact := {
      val art = (assembly / artifact).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(assembly / artifact, assembly),

    // Unidoc setting
    unidocSourceFilePatterns += SourceFilePattern("io/delta/standalone/"),
    javaCheckstyleSettings("dev/connectors-checkstyle.xml")
  ).configureUnidoc()


/*
TODO (TD): Tests are failing for some reason
lazy val compatibility = (project in file("connectors/oss-compatibility-tests"))
  // depend on standalone test codes as well
  .dependsOn(standalone % "compile->compile;test->test")
  .dependsOn(spark % "test -> compile")
  .settings(
    name := "compatibility",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      // Test Dependencies
      "io.netty" % "netty-buffer"  % "4.1.63.Final" % "test",
      "org.scalatest" %% "scalatest" % "3.1.0" % "test",
      "commons-io" % "commons-io" % "2.8.0" % "test",
      "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test",
      "org.apache.spark" %% "spark-catalyst" % defaultSparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % defaultSparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test" classifier "tests",
    )
  )
 */

lazy val goldenTables = (project in file("connectors/golden-tables"))
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings(
    name := "golden-tables",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      // Test Dependencies
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "commons-io" % "commons-io" % "2.8.0" % "test",

      "io.delta" %% "delta-spark" % "3.3.2" % "test",
      "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test",
      "org.apache.spark" %% "spark-catalyst" % defaultSparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % defaultSparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test" classifier "tests"
    )
  )

def sqlDeltaImportScalaVersion(scalaBinaryVersion: String): String = {
  scalaBinaryVersion match {
    // sqlDeltaImport doesn't support 2.11. We return 2.12 so that we can resolve the dependencies
    // but we will not publish sqlDeltaImport with Scala 2.11.
    case "2.11" => "2.12"
    case _ => scalaBinaryVersion
  }
}

lazy val sqlDeltaImport = (project in file("connectors/sql-delta-import"))
  .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
  .settings (
    name := "sql-delta-import",
    commonSettings,
    skipReleaseSettings,
    publishArtifact := scalaBinaryVersion.value != "2.11",
    Test / publishArtifact := false,
    libraryDependencies ++= Seq(
      // Using released delta-spark JAR instead of module dependency to break circular dependency
      "io.delta" %% "delta-spark" % "3.3.2",
      
      "io.netty" % "netty-buffer"  % "4.1.63.Final" % "test",
      "org.apache.spark" % ("spark-sql_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % defaultSparkVersion % "provided",
      "org.rogach" %% "scallop" % "3.5.1",
      "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test",
      "com.h2database" % "h2" % "1.4.200" % "test",
      "org.apache.spark" % ("spark-catalyst_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % defaultSparkVersion % "test",
      "org.apache.spark" % ("spark-core_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % defaultSparkVersion % "test",
      "org.apache.spark" % ("spark-sql_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % defaultSparkVersion % "test"
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
 ******************
 * Project groups *
 ******************
 */

// Don't use these groups for any other projects
lazy val sparkGroup = project
  .aggregate(spark, sparkV1, sparkV1Filtered, sparkV2, contribs, storage, storageS3DynamoDB, sharing, hudi)
  .settings(
    // crossScalaVersions must be set to Nil on the aggregating project
    crossScalaVersions := Nil,
    publishArtifact := false,
    publish / skip := false,
  )

lazy val icebergGroup = project
  .aggregate(iceberg, testDeltaIcebergJar)
  .settings(
    // crossScalaVersions must be set to Nil on the aggregating project
    crossScalaVersions := Nil,
    publishArtifact := false,
    publish / skip := false,
  )

lazy val kernelGroup = project
  .aggregate(kernelApi, kernelDefaults)
  .settings(
    // crossScalaVersions must be set to Nil on the aggregating project
    crossScalaVersions := Nil,
    publishArtifact := false,
    publish / skip := false,
    unidocSourceFilePatterns := {
      (kernelApi / unidocSourceFilePatterns).value.scopeToProject(kernelApi) ++
      (kernelDefaults / unidocSourceFilePatterns).value.scopeToProject(kernelDefaults)
    }
  ).configureUnidoc(docTitle = "Delta Kernel")

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


// Release settings for artifact that contains only Java source code
lazy val javaOnlyReleaseSettings = releaseSettings ++ Seq(
  // drop off Scala suffix from artifact names
  crossPaths := false,

  // we publish jars for each scalaVersion in crossScalaVersions. however, we only need to publish
  // one java jar. thus, only do so when the current scala version == default scala version
  publishArtifact := {
    val (expMaj, expMin, _) = getMajorMinorPatch(default_scala_version.value)
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
        <developer>
          <id>scottsand-db</id>
          <name>Scott Sandre</name>
          <url>https://github.com/scottsand-db</url>
        </developer>
        <developer>
          <id>windpiger</id>
          <name>Jun Song</name>
          <url>https://github.com/windpiger</url>
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
  releaseStepCommand(if (crossSparkReleaseEnabled) "crossSparkRelease +publishSigned" else "+publishSigned"),
  setNextVersion,
  commitNextVersion
)
