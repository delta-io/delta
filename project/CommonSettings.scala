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
import xsbti.compile.CompileAnalysis

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import com.lightbend.sbt.JavaFormatterPlugin.autoImport._

import Unidoc._

/**
 * Common settings and version constants shared across all modules.
 */
object CommonSettings {

  // Scala versions
  val scala213 = "2.13.17"
  val all_scala_versions = Seq(scala213)
  val default_scala_version = settingKey[String]("Default Scala version")

  // Version constants
  val scalaTestVersion = "3.2.15"
  val scalaTestVersionForConnectors = "3.0.8"
  val hadoopVersion = "3.4.2"
  val defaultSparkVersion = SparkVersionSpec.DEFAULT.fullVersion

  // Java version detection
  lazy val javaVersion = sys.props.getOrElse("java.version", "Unknown")
  lazy val javaVersionInt = javaVersion.split("\\.")(0).toInt

  /** Enforce java code style on compile. */
  def javafmtCheckSettings: Seq[Def.Setting[Task[CompileAnalysis]]] = Seq(
    (Compile / compile) := ((Compile / compile) dependsOn (Compile / javafmtCheckAll)).value
  )

  /** Enforce scala code style on compile. */
  def scalafmtCheckSettings: Seq[Def.Setting[Task[CompileAnalysis]]] = Seq(
    (Compile / compile) := ((Compile / compile) dependsOn (Compile / scalafmtCheckAll)).value,
  )

  // Target JVM setting key (defined here for use in commonSettings)
  val targetJvm = settingKey[String]("Target JVM version")

  /**
   * Common settings applied to all projects.
   */
  lazy val commonSettings: Seq[Setting[_]] = Seq(
    organization := "io.delta",
    scalaVersion := default_scala_version.value,
    crossScalaVersions := all_scala_versions,
    fork := true,
    scalacOptions ++= Seq("-Ywarn-unused:imports"),
    javacOptions ++= {
      if (javaVersion.startsWith("1.8")) {
        Seq.empty
      } else {
        Seq("--release", targetJvm.value)
      }
    },

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
        Seq(
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
    unidocSourceFilePatterns := Nil,
  )

  /**
   * Settings for modules that should not be published.
   */
  lazy val skipReleaseSettings: Seq[Setting[_]] = Seq(
    publishArtifact := false,
    publish / skip := true
  )
}

