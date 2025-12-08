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
import sbtassembly.AssemblyPlugin.autoImport._
import com.lightbend.sbt.JavaFormatterPlugin
import org.scalafmt.sbt.ScalafmtPlugin
import Checkstyle._
import Common._

/**
 * Hudi integration for Delta.
 */
object HudiProjects {

  /**
   * Delta Hudi - connector for reading/writing Hudi tables.
   */
  lazy val hudi = (project in file("hudi"))
    .dependsOn(SparkProjects.spark % "compile->compile;test->test;provided->provided")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
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
}
