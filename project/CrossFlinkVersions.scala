/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import java.io.{File, PrintWriter}

import sbt._
import sbt.Keys._
import sbtrelease.ReleasePlugin.autoImport.ReleaseStep

object CrossFlinkVersions extends AutoPlugin {
  private val defaultVersion = "2.3.0"
  private val supportedVersions = Seq(defaultVersion)

  require(
    supportedVersions.map(compatibilityVersion).distinct.size == supportedVersions.size,
    "Only one patch release can be published for each Flink compatibility line")

  /** Selects the requested supported Flink version, falling back to the default build version. */
  def selectedVersion: String = {
    val input = sys.props.getOrElse("flinkVersion", defaultVersion)
    if (!supportedVersions.contains(input)) {
      throw new IllegalArgumentException(
        s"Invalid flinkVersion: $input. Valid values: ${supportedVersions.mkString(", ")}")
    }
    input
  }

  /** Returns the major.minor line used in the Maven artifact name. */
  def compatibilityVersion(fullVersion: String): String = {
    val parts = fullVersion.split("\\.")
    require(
      parts.length >= 2,
      s"Flink version must contain a major and minor version: $fullVersion")
    parts.take(2).mkString(".")
  }
  override def trigger = allRequirements

  /**
   * Adds release steps for every supported Flink version not already published by the initial
   * all-module release step.
   */
  def crossFlinkReleaseSteps(task: String): Seq[ReleaseStep] = {
    supportedVersions.filterNot(_ == defaultVersion).map { version =>
      { (state: State) =>
        val extracted = Project.extract(state)
        val baseDir = extracted.get(ThisBuild / Keys.baseDirectory)
        val command = Seq(
          s"${baseDir.getAbsolutePath}/build/sbt",
          s"-DflinkVersion=$version",
          s"flink/$task")

        println(s"[info] Publishing Flink module for Flink $version")
        val exitCode = scala.sys.process.Process(command, baseDir).!
        if (exitCode != 0) {
          sys.error(s"Publishing Flink module for Flink $version failed with exit code $exitCode")
        }
        state
      }: ReleaseStep
    }
  }

  override lazy val projectSettings = Seq(
    // Runs the same non-default Flink publication steps used by releaseProcess. This command lets
    // CI exercise the real release path with a non-releasing task such as publishM2.
    commands += Command.args("publishAdditionalFlinkVersions", "<task>") { (state, args) =>
      if (args.isEmpty) {
        sys.error(
          "Usage: publishAdditionalFlinkVersions <task>\n" +
            "Example: build/sbt \"publishAdditionalFlinkVersions publishM2\"")
      }
      crossFlinkReleaseSteps(args.mkString(" ")).foldLeft(state) { (currentState, step) =>
        step(currentState)
      }
    },
    // Exports the supported Flink versions so CI can build its test matrix from the SBT source of
    // truth instead of maintaining a separate version list in the workflow.
    commands += Command.command("exportFlinkVersionsJson") { state =>
      val extracted = Project.extract(state)
      val baseDir = extracted.get(ThisBuild / Keys.baseDirectory)
      val outputFile = new File(baseDir, "target/flink-versions.json")
      outputFile.getParentFile.mkdirs()

      val writer = new PrintWriter(outputFile)
      try {
        writer.println("[")
        supportedVersions.zipWithIndex.foreach { case (version, index) =>
          val comma = if (index < supportedVersions.size - 1) "," else ""
          writer.println("  {")
          writer.println(s"""    "fullVersion": "$version",""")
          writer.println(
            s"""    "compatibilityVersion": "${compatibilityVersion(version)}",""")
          writer.println(s"""    "isDefault": ${version == defaultVersion}""")
          writer.println(s"  }$comma")
        }
        writer.println("]")
      } finally {
        writer.close()
      }

      println(s"[info] Flink version information exported to: ${outputFile.getAbsolutePath}")
      state
    })
}
