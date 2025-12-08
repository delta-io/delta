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
import sbt.internal.inc.Analysis
import xsbti.compile.CompileAnalysis
import sbtassembly.AssemblyPlugin.autoImport._
import sbtprotoc.ProtocPlugin.autoImport._
import com.lightbend.sbt.JavaFormatterPlugin
import org.scalafmt.sbt.ScalafmtPlugin
import Common._

/**
 * Spark Connect integration for Delta - client/server implementation.
 */
object ConnectProjects {

  /**
   * Delta Connect Common - shared protobuf definitions for connect client/server.
   */
  lazy val connectCommon = (project in file("spark-connect/common"))
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-connect-common",
      commonSettings,
      CrossSparkVersions.sparkDependentSettings(sparkVersion),
      releaseSettings,
      Compile / compile := runTaskOnlyOnSparkMaster(
        task = Compile / compile,
        taskName = "compile",
        projectName = "delta-connect-common",
        emptyValue = Analysis.empty.asInstanceOf[CompileAnalysis]
      ).value,
      Test / test := runTaskOnlyOnSparkMaster(
        task = Test / test,
        taskName = "test",
        projectName = "delta-connect-common",
        emptyValue = ()).value,
      publish := runTaskOnlyOnSparkMaster(
        task = publish,
        taskName = "publish",
        projectName = "delta-connect-common",
        emptyValue = ()).value,
      libraryDependencies ++= Seq(
        "io.grpc" % "protoc-gen-grpc-java" % grpcVersion asProtocPlugin(),
        "io.grpc" % "grpc-protobuf" % grpcVersion,
        "io.grpc" % "grpc-stub" % grpcVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf",
        "javax.annotation" % "javax.annotation-api" % "1.3.2",

        "org.apache.spark" %% "spark-connect-common" % sparkVersion.value % "provided",
      ),
      PB.protocVersion := protoVersion,
      Compile / PB.targets := Seq(
        PB.gens.java -> (Compile / sourceManaged).value,
        PB.gens.plugin("grpc-java") -> (Compile / sourceManaged).value
      ),
    )

  /**
   * Delta Connect Client - client-side Delta Connect implementation.
   */
  lazy val connectClient = (project in file("spark-connect/client"))
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .dependsOn(connectCommon % "compile->compile;test->test;provided->provided")
    .settings(
      name := "delta-connect-client",
      commonSettings,
      releaseSettings,
      CrossSparkVersions.sparkDependentSettings(sparkVersion),
      Compile / compile := runTaskOnlyOnSparkMaster(
        task = Compile / compile,
        taskName = "compile",
        projectName = "delta-connect-client",
        emptyValue = Analysis.empty.asInstanceOf[CompileAnalysis]
      ).value,
      Test / test := runTaskOnlyOnSparkMaster(
        task = Test / test,
        taskName = "test",
        projectName = "delta-connect-client",
        emptyValue = ()
      ).value,
      publish := runTaskOnlyOnSparkMaster(
        task = publish,
        taskName = "publish",
        projectName = "delta-connect-client",
        emptyValue = ()
      ).value,
      libraryDependencies ++= Seq(
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf",
        "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion.value % "provided",

        // Test deps
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion.value % "test" classifier "tests"
      ),
      (Test / javaOptions) += {
        // Create a (mini) Spark Distribution based on the server classpath.
        val serverClassPath = (connectServer / Compile / fullClasspath).value
        val distributionDir = crossTarget.value / "test-dist"
        if (!distributionDir.exists()) {
          val jarsDir = distributionDir / "jars"
          IO.createDirectory(jarsDir)
          // Create symlinks for all dependencies
          serverClassPath.distinct.foreach { entry =>
            val jarFile = entry.data.toPath
            val linkedJarFile = jarsDir / entry.data.getName
            Files.createSymbolicLink(linkedJarFile.toPath, jarFile)
          }
          // Create a symlink for the log4j properties
          val confDir = distributionDir / "conf"
          IO.createDirectory(confDir)
          val log4jProps = (SparkProjects.spark / Test / resourceDirectory).value / "log4j2_spark_master.properties"
          val linkedLog4jProps = confDir / "log4j2.properties"
          Files.createSymbolicLink(linkedLog4jProps.toPath, log4jProps.toPath)
        }
        // Return the location of the distribution directory.
        "-Ddelta.spark.home=" + distributionDir
      },
      // Required for testing addFeatureSupport/dropFeatureSupport.
      Test / envVars += ("DELTA_TESTING", "1"),
    )

  /**
   * Delta Connect Server - server-side Delta Connect implementation.
   */
  lazy val connectServer = (project in file("spark-connect/server"))
    .dependsOn(connectCommon % "compile->compile;test->test;provided->provided")
    .dependsOn(SparkProjects.spark % "compile->compile;test->test;provided->provided")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-connect-server",
      commonSettings,
      releaseSettings,
      CrossSparkVersions.sparkDependentSettings(sparkVersion),
      assembly / assemblyMergeStrategy := {
        // Discard module-info.class files from Java 9+ modules and multi-release JARs
        case "module-info.class" => MergeStrategy.discard
        case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.discard
        case x =>
          val oldStrategy = (assembly / assemblyMergeStrategy).value
          oldStrategy(x)
      },
      Compile / compile := runTaskOnlyOnSparkMaster(
        task = Compile / compile,
        taskName = "compile",
        projectName = "delta-connect-server",
        emptyValue = Analysis.empty.asInstanceOf[CompileAnalysis]
      ).value,
      Test / test := runTaskOnlyOnSparkMaster(
        task = Test / test,
        taskName = "test",
        projectName = "delta-connect-server",
        emptyValue = ()
      ).value,
      publish := runTaskOnlyOnSparkMaster(
        task = publish,
        taskName = "publish",
        projectName = "delta-connect-server",
        emptyValue = ()
      ).value,
      libraryDependencies ++= Seq(
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf",

        "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided",
        "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
        "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
        "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",
        "org.apache.spark" %% "spark-connect" % sparkVersion.value % "provided",

        "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
        "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
        "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
        "org.apache.spark" %% "spark-hive" % sparkVersion.value % "test" classifier "tests",
        "org.apache.spark" %% "spark-connect" % sparkVersion.value % "test" classifier "tests",
      ),
      excludeDependencies ++= Seq(
        // Exclude connect common because a properly shaded version of it is included in the
        // spark-connect jar. Including it causes classpath problems.
        ExclusionRule("org.apache.spark", "spark-connect-common_2.13"),
        // Exclude connect shims because we have spark-core on the classpath. The shims are only
        // needed for the client. Including it causes classpath problems.
        ExclusionRule("org.apache.spark", "spark-connect-shims_2.13")
      ),
      // Required for testing addFeatureSupport/dropFeatureSupport.
      Test / envVars += ("DELTA_TESTING", "1"),
    )
}
