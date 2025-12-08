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
import sbt.internal.inc.Analysis
import xsbti.compile.CompileAnalysis
import com.lightbend.sbt.JavaFormatterPlugin
import org.scalafmt.sbt.ScalafmtPlugin
import Checkstyle._
import Common._
import Unidoc._

/**
 * Delta Sharing integration for Spark.
 */
object SharingProjects {

  /**
   * Delta Sharing Spark connector - read shared Delta tables.
   */
  lazy val sharing = (project in file("sharing"))
    .dependsOn(SparkProjects.spark % "compile->compile;test->test;provided->provided")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-sharing-spark",
      commonSettings,
      scalaStyleSettings,
      releaseSettings,
      CrossSparkVersions.sparkDependentSettings(sparkVersion),
      Test / javaOptions ++= Seq("-ea"),
      Compile / compile := runTaskOnlyOnSparkMaster(
        task = Compile / compile,
        taskName = "compile",
        projectName = "delta-sharing-spark",
        emptyValue = Analysis.empty.asInstanceOf[CompileAnalysis]
      ).value,
      Test / test := runTaskOnlyOnSparkMaster(
        task = Test / test,
        taskName = "test",
        projectName = "delta-sharing-spark",
        emptyValue = ()).value,
      publish := runTaskOnlyOnSparkMaster(
        task = publish,
        taskName = "publish",
        projectName = "delta-sharing-spark",
        emptyValue = ()).value,
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",

        "io.delta" %% "delta-sharing-client" % "1.3.6",

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
}
