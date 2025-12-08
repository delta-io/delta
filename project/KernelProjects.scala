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

import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import org.scalafmt.sbt.ScalafmtPlugin
import Checkstyle._
import Common._
import Unidoc._

/**
 * Delta Kernel projects - standalone Java library for reading/writing Delta tables.
 */
object KernelProjects {

  /**
   * Delta Kernel API - the core public API for reading/writing Delta tables.
   * This is a Java-only library that gets shaded with Jackson to avoid version conflicts.
   */
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

      // Also publish a test-jar (classifier = "tests") so consumers (e.g. kernelDefault)
      // can depend on test utilities via a published artifact instead of depending on raw class directories.
      Test / publishArtifact := true,
      Test / packageBin / artifactClassifier := Some("tests"),
      libraryDependencies ++= Seq(
        "org.roaringbitmap" % "RoaringBitmap" % "0.9.25",
        "org.slf4j" % "slf4j-api" % "1.7.36",

        "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5",
        "com.fasterxml.jackson.core" % "jackson-core" % "2.13.5",
        "com.fasterxml.jackson.core" % "jackson-annotations" % "2.13.5",
        "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.13.5",

        // JSR-305 annotations for @Nullable
        "com.google.code.findbugs" % "jsr305" % "3.0.2",

        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "junit" % "junit" % "4.13.2" % "test",
        "com.novocode" % "junit-interface" % "0.11" % "test",
        "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % "test",
        "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % "test",
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

  /**
   * Delta Kernel Defaults - default implementations for Kernel APIs.
   * Provides Hadoop-based file system access, Parquet readers, etc.
   */
  lazy val kernelDefaults = (project in file("kernel/kernel-defaults"))
    .enablePlugins(ScalafmtPlugin)
    .dependsOn(StorageProjects.storage)
    .dependsOn(StorageProjects.storage % "test->test") // Required for InMemoryCommitCoordinator for tests
    .dependsOn(ConnectorProjects.goldenTables % "test")
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

      // Put the shaded kernel-api JAR on the classpath (compile & test)
      Compile / unmanagedJars += (kernelApi / Compile / packageBin).value,
      Test / unmanagedJars += (kernelApi / Compile / packageBin).value,

      // Make sure the shaded JAR is produced before we compile/run tests
      Compile / compile := (Compile / compile).dependsOn(kernelApi / Compile / packageBin).value,
      Test / test       := (Test    / test).dependsOn(kernelApi / Compile / packageBin).value,
      Test / unmanagedJars += (kernelApi / Test / packageBin).value,

      libraryDependencies ++= Seq(
        "org.assertj" % "assertj-core" % "3.26.3" % Test,
        "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5",
        "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.13.5",
        "org.apache.parquet" % "parquet-hadoop" % "1.12.3",

        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "junit" % "junit" % "4.13.2" % "test",
        "commons-io" % "commons-io" % "2.8.0" % "test",
        "com.novocode" % "junit-interface" % "0.11" % "test",
        "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % "test",
        "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % "test",
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

  /**
   * Benchmarks for Kernel - not published, internal testing only.
   */
  lazy val kernelBenchmarks = (project in file("kernel/kernel-benchmarks"))
    .enablePlugins(ScalafmtPlugin)
    .dependsOn(kernelDefaults % "test->test")
    .dependsOn(kernelApi % "test->test")
    .dependsOn(StorageProjects.storage % "test->test")
    .dependsOn(kernelUnityCatalog % "test->test")
    .settings(
      name := "delta-kernel-benchmarks",
      commonSettings,
      skipReleaseSettings,
      exportJars := false,
      javafmtCheckSettings,
      scalafmtCheckSettings,

      libraryDependencies ++= Seq(
        "org.openjdk.jmh" % "jmh-core" % "1.37" % "test",
        "org.openjdk.jmh" % "jmh-generator-annprocess" % "1.37" % "test",
      ),
    )

  /**
   * Unity Catalog integration for Kernel.
   */
  lazy val kernelUnityCatalog = (project in file("kernel/unitycatalog"))
    .enablePlugins(ScalafmtPlugin)
    .dependsOn(kernelDefaults % "test->test")
    .dependsOn(StorageProjects.storage)
    .settings(
      name := "delta-kernel-unitycatalog",
      commonSettings,
      javaOnlyReleaseSettings,
      javafmtCheckSettings,
      javaCheckstyleSettings("dev/kernel-checkstyle.xml"),
      scalaStyleSettings,
      scalafmtCheckSettings,

      // Put the shaded kernel-api JAR on the classpath (compile & test)
      Compile / unmanagedJars += (kernelApi / Compile / packageBin).value,
      Test / unmanagedJars += (kernelApi / Compile / packageBin).value,

      // Make sure the shaded JAR is produced before we compile/run tests
      Compile / compile := (Compile / compile).dependsOn(kernelApi / Compile / packageBin).value,
      Test / test       := (Test    / test).dependsOn(kernelApi / Compile / packageBin).value,
      Test / unmanagedJars += (kernelApi / Test / packageBin).value,

      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % "test",
        "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % "test",
      ),
      unidocSourceFilePatterns += SourceFilePattern("src/main/java/io/delta/unity/"),
    ).configureUnidoc()

  /**
   * Aggregator project for all kernel modules.
   * Use this to build/test/publish all kernel artifacts together.
   */
  lazy val kernelGroup = project
    .aggregate(kernelApi, kernelDefaults, kernelBenchmarks)
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
}
