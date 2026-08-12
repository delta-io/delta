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

object FlinkVersionSpec {
  val DEFAULT: String = "2.0.2"
  val SUPPORTED: Seq[String] = Seq(DEFAULT)

  require(SUPPORTED.headOption.contains(DEFAULT), "The default Flink version must be supported")
  require(
    SUPPORTED.map(compatibilityVersion).distinct.size == SUPPORTED.size,
    "Only one patch release can be published for each Flink compatibility line")

  def selectedVersion: String = sys.props.getOrElse("flinkVersion", DEFAULT)

  def compatibilityVersion(fullVersion: String): String = {
    val parts = fullVersion.split("\\.")
    require(
      parts.length >= 2,
      s"Flink version must contain a major and minor version: $fullVersion")
    parts.take(2).mkString(".")
  }
}

object CrossFlinkVersions extends AutoPlugin {
  override def trigger = allRequirements

  /**
   * Adds release steps for every supported Flink version not already published by the initial
   * all-module release step.
   */
  def crossFlinkReleaseSteps(task: String): Seq[ReleaseStep] = {
    FlinkVersionSpec.SUPPORTED.filterNot(_ == FlinkVersionSpec.DEFAULT).map { version =>
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
    commands += Command.command("exportFlinkVersionsJson") { state =>
      val extracted = Project.extract(state)
      val baseDir = extracted.get(ThisBuild / Keys.baseDirectory)
      val outputFile = new File(baseDir, "target/flink-versions.json")
      outputFile.getParentFile.mkdirs()

      val writer = new PrintWriter(outputFile)
      try {
        writer.println("[")
        FlinkVersionSpec.SUPPORTED.zipWithIndex.foreach { case (version, index) =>
          val comma = if (index < FlinkVersionSpec.SUPPORTED.size - 1) "," else ""
          writer.println("  {")
          writer.println(s"""    "fullVersion": "$version",""")
          writer.println(
            s"""    "compatibilityVersion": "${FlinkVersionSpec.compatibilityVersion(version)}",""")
          writer.println(s"""    "isDefault": ${version == FlinkVersionSpec.DEFAULT}""")
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
