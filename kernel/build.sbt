/*
 * Copyright (2024) The Delta Lake Project Authors.
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

// Dependency versions
val hadoopVersion = "3.4.0"
val scalaTestVersion = "3.2.15"
val jacksonVersion = "2.13.5"

val scala213 = "2.13.16"

// Common settings for all kernel modules
lazy val commonSettings = Seq(
  organization := "io.delta",
  // Java-only modules - no Scala version suffix in artifact names
  crossPaths := false,
  autoScalaLibrary := false,
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala213),
  // Target Java 8 for compatibility
  javacOptions ++= Seq("-source", "8", "-target", "8", "-Xlint:all"),
  // Publishing settings
  publishMavenStyle := true,
  publishTo := sonatypePublishToBundle.value,
  // Test settings
  Test / fork := true,
  Test / javaOptions ++= Seq("-ea", "-Xmx1024m"),
)

// Settings for Java-only release (no Scala artifacts)
lazy val javaOnlyReleaseSettings = Seq(
  crossPaths := false,
  autoScalaLibrary := false,
  // Only publish for default Scala version
  crossScalaVersions := Seq(scalaVersion.value),
)

// Jackson shading configuration for kernelApi
lazy val kernelApiAssemblySettings = Seq(
  // Replace the default packageBin with the shaded assembly jar
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
    ShadeRule.rename("com.fasterxml.jackson.**" ->
      "io.delta.kernel.shaded.com.fasterxml.jackson.@1").inAll
  ),
  assembly / assemblyMergeStrategy := {
    case "module-info.class" => MergeStrategy.discard
    case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
    case PathList("META-INF", "services", xs @ _*) => MergeStrategy.discard
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
)

// Generate Meta.java with version information
lazy val generateMetaSettings = Seq(
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
)

// ============================================================================
// Kernel API - Core API with shaded Jackson
// ============================================================================
lazy val kernelApi = (project in file("kernel-api"))
  .settings(
    name := "delta-kernel-api",
    commonSettings,
    javaOnlyReleaseSettings,
    coverageEnabled := false,
    kernelApiAssemblySettings,
    generateMetaSettings,
    // Publish test jar for consumers
    Test / publishArtifact := true,
    Test / packageBin / artifactClassifier := Some("tests"),
    libraryDependencies ++= Seq(
      "org.roaringbitmap" % "RoaringBitmap" % "0.9.25",
      "org.slf4j" % "slf4j-api" % "1.7.36",
      // Jackson (will be shaded)
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jacksonVersion,
      // JSR-305 annotations for @Nullable
      "com.google.code.findbugs" % "jsr305" % "3.0.2",
      // Test dependencies
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "junit" % "junit" % "4.13.2" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % "test",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % "test",
      "org.assertj" % "assertj-core" % "3.26.3" % "test",
      // JMH for benchmarks in tests
      "org.openjdk.jmh" % "jmh-core" % "1.37" % "test",
      "org.openjdk.jmh" % "jmh-generator-annprocess" % "1.37" % "test",
    ),
  )

// ============================================================================
// Storage - Storage abstraction layer
// NOTE: Storage must be physically moved to kernel/storage/ for this to work
// ============================================================================
lazy val storage = (project in file("storage"))
  .settings(
    name := "delta-storage",
    commonSettings,
    javaOnlyReleaseSettings,
    exportJars := true,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "provided",
      // Test dependencies
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    ),
  )

// ============================================================================
// Kernel Defaults - Default implementations
// ============================================================================
lazy val kernelDefaults = (project in file("kernel-defaults"))
  .dependsOn(kernelApi)
  .dependsOn(kernelApi % "test->test")
  .dependsOn(storage)
  .dependsOn(storage % "test->test")
  .settings(
    name := "delta-kernel-defaults",
    commonSettings,
    javaOnlyReleaseSettings,
    // Put the shaded kernel-api JAR on the classpath
    Compile / unmanagedJars += (kernelApi / Compile / packageBin).value,
    Test / unmanagedJars += (kernelApi / Compile / packageBin).value,
    // Make sure the shaded JAR is produced before we compile/run tests
    Compile / compile := (Compile / compile).dependsOn(kernelApi / Compile / packageBin).value,
    Test / test := (Test / test).dependsOn(kernelApi / Compile / packageBin).value,
    Test / unmanagedJars += (kernelApi / Test / packageBin).value,
    Test / envVars += ("DELTA_TESTING", "1"),
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jacksonVersion,
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3",
      // Test dependencies (NO delta-spark!)
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "junit" % "junit" % "4.13.2" % "test",
      "commons-io" % "commons-io" % "2.8.0" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % "test",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % "test",
      "org.assertj" % "assertj-core" % "3.26.3" % "test",
    ),
  )

// ============================================================================
// Kernel Unity Catalog - Unity Catalog integration
// ============================================================================
lazy val kernelUnityCatalog = (project in file("unitycatalog"))
  .dependsOn(kernelDefaults)
  .dependsOn(kernelDefaults % "test->test")
  .dependsOn(storage)
  .settings(
    name := "delta-kernel-unitycatalog",
    commonSettings,
    javaOnlyReleaseSettings,
    // Put the shaded kernel-api JAR on the classpath
    Compile / unmanagedJars += (kernelApi / Compile / packageBin).value,
    Test / unmanagedJars += (kernelApi / Compile / packageBin).value,
    // Make sure the shaded JAR is produced before we compile/run tests
    Compile / compile := (Compile / compile).dependsOn(kernelApi / Compile / packageBin).value,
    Test / test := (Test / test).dependsOn(kernelApi / Compile / packageBin).value,
    Test / unmanagedJars += (kernelApi / Test / packageBin).value,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
      // Test dependencies
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % "test",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % "test",
    ),
  )

// ============================================================================
// Root project - aggregates all kernel modules
// ============================================================================
lazy val root = (project in file("."))
  .aggregate(kernelApi, kernelDefaults, storage, kernelUnityCatalog)
  .settings(
    name := "delta-kernel",
    publish / skip := true,
    // Don't publish the root project
    publishArtifact := false,
  )
