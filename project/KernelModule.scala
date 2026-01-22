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
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

import org.scalafmt.sbt.ScalafmtPlugin
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

import Checkstyle._
import CommonSettings._
import Unidoc._

/**
 * Kernel module project definitions.
 * 
 * This object contains all kernel-related project definitions.
 * All projects in the kernel group are defined here.
 */
object KernelModule {

  // References to projects defined in build.sbt
  private def storage = LocalProject("storage")
  private def goldenTables = LocalProject("goldenTables")

  /**
   * Delta Kernel API - the core kernel interfaces and types.
   * This module is shaded to avoid Jackson version conflicts.
   */
  lazy val kernelApi: Project = {
    (Project("kernelApi", file("kernel/kernel-api")))
      .enablePlugins(ScalafmtPlugin)
      .settings(
        name := "delta-kernel-api",
        commonSettings,
        scalaStyleSettings,
        javaOnlyReleaseSettings,
        javafmtCheckSettings,
        scalafmtCheckSettings,

        Compile / classDirectory := target.value / "scala-2.13" / "kernel-api-classes",

        Test / javaOptions ++= Seq("-ea"),

        Test / publishArtifact := true,
        Test / packageBin / artifactClassifier := Some("tests"),
        libraryDependencies ++= Seq(
          "org.roaringbitmap" % "RoaringBitmap" % "0.9.25",
          "org.slf4j" % "slf4j-api" % "1.7.36",

          "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5",
          "com.fasterxml.jackson.core" % "jackson-core" % "2.13.5",
          "com.fasterxml.jackson.core" % "jackson-annotations" % "2.13.5",
          "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.13.5",

          "com.google.code.findbugs" % "jsr305" % "3.0.2",

          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "junit" % "junit" % "4.13.2" % "test",
          "com.novocode" % "junit-interface" % "0.11" % "test",
          "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % "test",
          "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % "test",
          "org.assertj" % "assertj-core" % "3.26.3" % "test",
          "org.openjdk.jmh" % "jmh-core" % "1.37" % "test",
          "org.openjdk.jmh" % "jmh-generator-annprocess" % "1.37" % "test"
        ),
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
          case "module-info.class" => MergeStrategy.discard
          case PathList("META-INF", "services", xs @ _*) => MergeStrategy.discard
          case x =>
            val oldStrategy = (assembly / assemblyMergeStrategy).value
            oldStrategy(x)
        },
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
               |    public static final String KERNEL_VERSION = "$${version.value}";
               |}
               |""".stripMargin)
          Seq(file)
        },
        MultiShardMultiJVMTestParallelization.settings,
        javaCheckstyleSettings("dev/kernel-checkstyle.xml"),
        unidocSourceFilePatterns := Seq(SourceFilePattern("io/delta/kernel/")),
      ).configureUnidoc("Delta Kernel")
  }

  /**
   * Delta Kernel Defaults - default implementations for the kernel interfaces.
   */
  lazy val kernelDefaults: Project = {
    (Project("kernelDefaults", file("kernel/kernel-defaults")))
      .enablePlugins(ScalafmtPlugin)
      .dependsOn(storage)
      .dependsOn(storage % "test->test")
      .dependsOn(goldenTables % "test")
      .settings(
        name := "delta-kernel-defaults",
        commonSettings,
        scalaStyleSettings,
        javaOnlyReleaseSettings,
        javafmtCheckSettings,
        scalafmtCheckSettings,

        Compile / classDirectory := target.value / "scala-2.13" / "kernel-defaults-classes",

        Test / javaOptions ++= Seq("-ea"),
        Test / envVars += ("DELTA_TESTING", "1"),

        Compile / unmanagedJars += (kernelApi / Compile / packageBin).value,
        Test / unmanagedJars += (kernelApi / Compile / packageBin).value,

        Compile / compile := (Compile / compile).dependsOn(kernelApi / Compile / packageBin).value,
        Test / test := (Test / test).dependsOn(kernelApi / Compile / packageBin).value,
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
          "org.openjdk.jmh" % "jmh-core" % "1.37" % "test",
          "org.openjdk.jmh" % "jmh-generator-annprocess" % "1.37" % "test",
          "io.delta" %% "delta-spark" % "4.0.0" % "test",

          "org.apache.spark" %% "spark-hive" % defaultSparkVersion % "test" classifier "tests",
          "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test" classifier "tests",
          "org.apache.spark" %% "spark-core" % defaultSparkVersion % "test" classifier "tests",
          "org.apache.spark" %% "spark-catalyst" % defaultSparkVersion % "test" classifier "tests",
        ),
        MultiShardMultiJVMTestParallelization.settings,
        javaCheckstyleSettings("dev/kernel-checkstyle.xml"),
        unidocSourceFilePatterns += SourceFilePattern("io/delta/kernel/"),
      ).configureUnidoc("Delta Kernel Defaults")
  }

  /**
   * Delta Kernel Benchmarks - performance benchmarks for the kernel.
   */
  lazy val kernelBenchmarks: Project = {
    (Project("kernelBenchmarks", file("kernel/kernel-benchmarks")))
      .enablePlugins(ScalafmtPlugin)
      .dependsOn(kernelDefaults % "test->test")
      .dependsOn(kernelApi % "test->test")
      .dependsOn(storage % "test->test")
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
  }

  /**
   * Delta Kernel Unity Catalog - Unity Catalog integration for the kernel.
   */
  lazy val kernelUnityCatalog: Project = {
    (Project("kernelUnityCatalog", file("kernel/unitycatalog")))
      .enablePlugins(ScalafmtPlugin)
      .dependsOn(kernelDefaults % "test->test")
      .dependsOn(storage)
      .settings(
        name := "delta-kernel-unitycatalog",
        commonSettings,
        javaOnlyReleaseSettings,
        javafmtCheckSettings,
        javaCheckstyleSettings("dev/kernel-checkstyle.xml"),
        scalaStyleSettings,
        scalafmtCheckSettings,

        Compile / unmanagedJars += (kernelApi / Compile / packageBin).value,
        Test / unmanagedJars += (kernelApi / Compile / packageBin).value,

        Compile / compile := (Compile / compile).dependsOn(kernelApi / Compile / packageBin).value,
        Test / test := (Test / test).dependsOn(kernelApi / Compile / packageBin).value,
        Test / unmanagedJars += (kernelApi / Test / packageBin).value,

        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % "test",
          "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % "test",
        ),
        unidocSourceFilePatterns += SourceFilePattern("src/main/java/io/delta/unity/"),
      ).configureUnidoc()
  }

  /**
   * Aggregate project for all kernel modules.
   */
  lazy val kernelGroup: Project = {
    (Project("kernelGroup", file("kernelGroup")))
      .aggregate(kernelApi, kernelDefaults, kernelBenchmarks)
      .settings(
        crossScalaVersions := Nil,
        publishArtifact := false,
        publish / skip := false,
        unidocSourceFilePatterns := {
          (kernelApi / unidocSourceFilePatterns).value.scopeToProject(kernelApi) ++
            (kernelDefaults / unidocSourceFilePatterns).value.scopeToProject(kernelDefaults)
        }
      ).configureUnidoc("Delta Kernel")
  }
  // Note: javaOnlyReleaseSettings is imported from CommonSettings
}
