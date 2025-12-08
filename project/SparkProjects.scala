/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import sbt._
import Keys._
import java.nio.file.Files
import com.simplytyped.Antlr4Plugin
import com.simplytyped.Antlr4Plugin.autoImport._
import com.lightbend.sbt.JavaFormatterPlugin
import org.scalafmt.sbt.ScalafmtPlugin
import com.typesafe.tools.mima.plugin.MimaPlugin.autoImport._
import Checkstyle._
import Common._
import Mima._
import Unidoc._
import TestParallelization._

/**
 * Delta Spark projects - the main Delta Lake implementation for Apache Spark.
 */
object SparkProjects {

  /**
   * Delta Suite Generator - internal tool for generating modular test suites.
   */
  lazy val deltaSuiteGenerator = (project in file("spark/delta-suite-generator"))
    .disablePlugins(ScalafmtPlugin)
    .settings(
      name := "delta-suite-generator",
      commonSettings,
      scalaStyleSettings,
      skipReleaseSettings, // Internal module - not published to Maven
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
  /**
   * Delta Spark V1 - traditional Delta implementation (production code only).
   * Tests are compiled in the final 'spark' module to avoid circular dependencies.
   */
  lazy val sparkV1 = (project in file("spark"))
    .dependsOn(StorageProjects.storage)
    .enablePlugins(Antlr4Plugin)
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-spark-v1",
      commonSettings,
      scalaStyleSettings,
      skipReleaseSettings, // Internal module - not published to Maven
      CrossSparkVersions.sparkDependentSettings(sparkVersion),

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
  /**
   * Delta Spark V1 Filtered - sparkV1 without DeltaLog/Snapshot classes.
   * Used by sparkV2 to access utilities without core Delta V1 implementation.
   */
  lazy val sparkV1Filtered = (project in file("spark-v1-filtered"))
    .dependsOn(sparkV1)
    .dependsOn(StorageProjects.storage)
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
  /**
   * Delta Spark V2 - Kernel-based Delta implementation.
   * Uses Delta Kernel APIs for reading/writing Delta tables.
   */
  lazy val sparkV2 = (project in file("kernel-spark"))
    .dependsOn(sparkV1Filtered)
    .dependsOn(KernelProjects.kernelDefaults)
    .dependsOn(ConnectorProjects.goldenTables % "test")
    .settings(
      name := "delta-spark-v2",
      commonSettings,
      javafmtCheckSettings,
      skipReleaseSettings, // Internal module - not published to Maven
      CrossSparkVersions.sparkDependentSettings(sparkVersion),
      exportJars := true,  // Export as JAR to avoid classpath conflicts

      Test / javaOptions ++= Seq("-ea"),
      // make sure shaded kernel-api jar exists before compiling/testing
      Compile / compile := (Compile / compile)
        .dependsOn(KernelProjects.kernelApi / Compile / packageBin).value,
      Test / test := (Test / test)
        .dependsOn(KernelProjects.kernelApi / Compile / packageBin).value,
      Test / unmanagedJars += (KernelProjects.kernelApi / Test / packageBin).value,
      Compile / unmanagedJars ++= Seq(
        (KernelProjects.kernelApi / Compile / packageBin).value
      ),
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
  /**
   * Delta Spark - unified Delta implementation combining V1 and V2.
   * This is the main artifact published to Maven as delta-spark.
   */
  lazy val spark = (project in file("spark-unified"))
    .dependsOn(sparkV1)
    .dependsOn(sparkV2)
    .dependsOn(StorageProjects.storage)
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-spark",
      commonSettings,
      scalaStyleSettings,
      sparkMimaSettings,
      releaseSettings, // Published to Maven as delta-spark.jar

      // Set Test baseDirectory before sparkDependentSettings() so it uses the correct directory
      Test / baseDirectory := (sparkV1 / baseDirectory).value,

      // Test sources from spark/ directory (sparkV1's directory) AND spark-unified's own directory
      // MUST be set BEFORE crossSparkSettings() to avoid overwriting version-specific directories
      Test / unmanagedSourceDirectories := {
        val sparkDir = (sparkV1 / baseDirectory).value
        val unifiedDir = baseDirectory.value
        Seq(
          sparkDir / "src" / "test" / "scala",
          sparkDir / "src" / "test" / "java",
          unifiedDir / "src" / "test" / "scala",
          unifiedDir / "src" / "test" / "java"
        )
      },
      Test / unmanagedResourceDirectories := Seq(
        (sparkV1 / baseDirectory).value / "src" / "test" / "resources",
        baseDirectory.value / "src" / "test" / "resources"
      ),

      CrossSparkVersions.sparkDependentSettings(sparkVersion),

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
              // Check if artifactId starts with any internal module name
              // (e.g., "delta-spark-v1_4.1_2.13" starts with "delta-spark-v1")
              val isInternal = internalModules.exists(module => artifactId.startsWith(module))
              if (isInternal) Seq.empty else Seq(n)
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
      generatedJavaDoc = CrossSparkVersions.getSparkVersionSpec().generateDocs,
      generateScalaDoc = CrossSparkVersions.getSparkVersionSpec().generateDocs,
      // spark-connect has classes with the same name as spark-core, this causes compilation issues
      // with unidoc since it concatenates the classpaths from all modules
      // ==> thus we exclude such sources
      // (mostly) relevant github issue: https://github.com/sbt/sbt-unidoc/issues/77
      classPathToSkip = "spark-connect"
    )

  /**
   * Delta Contribs - community contributions and experimental features.
   */
  lazy val contribs = (project in file("contribs"))
    .dependsOn(spark % "compile->compile;test->test;provided->provided")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-contribs",
      commonSettings,
      scalaStyleSettings,
      releaseSettings,
      CrossSparkVersions.sparkDependentModuleName(sparkVersion),
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

  /**
   * Aggregator for all Spark-related projects.
   */
  lazy val sparkGroup = project
    .aggregate(spark, sparkV1, sparkV1Filtered, sparkV2, contribs, StorageProjects.storage, StorageProjects.storageS3DynamoDB, SharingProjects.sharing, HudiProjects.hudi)
    .settings(
      // crossScalaVersions must be set to Nil on the aggregating project
      crossScalaVersions := Nil,
      publishArtifact := false,
      publish / skip := false,
    )
}
