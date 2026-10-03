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

case class FlinkVersionSpec(fullVersion: String) {

  /** Returns the Flink compatibility version used in the Maven artifact name. */
  def shortVersion: String = {
    val (major, minor, _) = Mima.getMajorMinorPatch(fullVersion)
    s"$major.$minor"
  }
}

object FlinkVersionSpec {
  private val defaultVersion = "2.3.0"

  val DEFAULT = FlinkVersionSpec(defaultVersion)

  val ALL_SPECS =
    Seq("2.0.2", "2.1.3", "2.2.1", "2.3.0", defaultVersion)
      .distinct
      .map(FlinkVersionSpec.apply)

  require(
    ALL_SPECS.map(_.shortVersion).distinct.size == ALL_SPECS.size,
    "Only one patch release can be published for each Flink compatibility line")
}

object CrossFlinkVersions extends AutoPlugin {

  /** Returns the selected Flink version specification. */
  def getFlinkVersionSpec(): FlinkVersionSpec = {
    val input = sys.props.getOrElse("flinkVersion", FlinkVersionSpec.DEFAULT.fullVersion)
    FlinkVersionSpec.ALL_SPECS.find { spec =>
      spec.fullVersion == input || spec.shortVersion == input
    }.getOrElse {
      val validInputs = FlinkVersionSpec.ALL_SPECS.flatMap { spec =>
        Seq(spec.fullVersion, spec.shortVersion)
      }
      throw new IllegalArgumentException(
        s"Invalid flinkVersion: $input. Valid values: ${validInputs.mkString(", ")}")
    }
  }

  /** Returns the Maven artifact version used for Apache Flink dependencies. */
  def getFlinkArtifactVersion(): String = getFlinkVersionSpec().fullVersion

  override def trigger = allRequirements

  /** Adds one release step for every supported Flink version. */
  def crossFlinkReleaseSteps(task: String): Seq[ReleaseStep] = {
    FlinkVersionSpec.ALL_SPECS.map { spec =>
      { (state: State) =>
        val extracted = Project.extract(state)
        val baseDir = extracted.get(ThisBuild / Keys.baseDirectory)
        val command = Seq(
          s"${baseDir.getAbsolutePath}/build/sbt",
          s"-DflinkVersion=${spec.fullVersion}",
          s"flink/$task")

        println(s"[info] Publishing Flink module for Flink ${spec.fullVersion}")
        val exitCode = scala.sys.process.Process(command, baseDir).!
        if (exitCode != 0) {
          sys.error(
            s"Publishing Flink module for Flink ${spec.fullVersion} failed with exit code $exitCode")
        }
        state
      }: ReleaseStep
    }
  }

  override lazy val projectSettings = Seq(
    // Runs the same Flink publication steps used by releaseProcess. This command lets CI exercise
    // the real release path with a non-releasing task such as publishM2.
    commands += Command.args("publishFlinkVersions", "<task>") { (state, args) =>
      if (args.isEmpty) {
        sys.error(
          "Usage: publishFlinkVersions <task>\n" +
            "Example: build/sbt \"publishFlinkVersions publishM2\"")
      }
      crossFlinkReleaseSteps(args.mkString(" ")).foldLeft(state) { (currentState, step) =>
        step(currentState)
      }
    },
    // Exports the supported Flink versions so CI can build its test matrix from the SBT source of
    // truth instead of maintaining a separate version list in the workflow.
    commands += Command.command("exportFlinkVersionsJson") { state =>
      val outputFile = new File("target/flink-versions.json")
      outputFile.getParentFile.mkdirs()

      val writer = new PrintWriter(outputFile)
      try {
        writer.println("[")
        FlinkVersionSpec.ALL_SPECS.zipWithIndex.foreach { case (spec, index) =>
          val comma = if (index < FlinkVersionSpec.ALL_SPECS.size - 1) "," else ""
          writer.println("  {")
          writer.println(s"""    "fullVersion": "${spec.fullVersion}",""")
          writer.println(s"""    "compatibilityVersion": "${spec.shortVersion}"""")
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
