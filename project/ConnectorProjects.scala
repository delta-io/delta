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
import com.lightbend.sbt.JavaFormatterPlugin
import org.scalafmt.sbt.ScalafmtPlugin
import Checkstyle._
import Common._
import Mima._
import Unidoc._

/**
 * Connector projects - Delta Standalone and related testing utilities.
 */
object ConnectorProjects {

  /**
   * Golden tables - test data generator for connector compatibility testing.
   */
  lazy val goldenTables = (project in file("connectors/golden-tables"))
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "golden-tables",
      commonSettings,
      skipReleaseSettings,
      libraryDependencies ++= Seq(
        // Test Dependencies
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "commons-io" % "commons-io" % "2.8.0" % "test",

        "io.delta" %% "delta-spark" % "3.3.2" % "test",
        "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test",
        "org.apache.spark" %% "spark-catalyst" % defaultSparkVersion % "test" classifier "tests",
        "org.apache.spark" %% "spark-core" % defaultSparkVersion % "test" classifier "tests",
        "org.apache.spark" %% "spark-sql" % defaultSparkVersion % "test" classifier "tests"
      )
    )

  /**
   * Delta Standalone - Java library for reading Delta tables without Spark.
   * This is the core implementation (unshaded).
   */
  lazy val standalone = (project in file("connectors/standalone"))
    .dependsOn(StorageProjects.storage % "compile->compile;provided->provided")
    .dependsOn(goldenTables % "test")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-standalone-original",
      commonSettings,
      skipReleaseSettings,
      standaloneMimaSettings,
      // When updating any dependency here, we should also review `pomPostProcess` in project
      // `standaloneCosmetic` and update it accordingly.
      libraryDependencies ++= scalaCollectionPar(scalaVersion.value) ++ Seq(
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
        "com.github.mjakubowski84" %% "parquet4s-core" % parquet4sVersion excludeAll (
          ExclusionRule("org.slf4j", "slf4j-api")
          ),
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.3",
        "org.json4s" %% "json4s-jackson" % "3.7.0-M11" excludeAll (
          ExclusionRule("com.fasterxml.jackson.core"),
          ExclusionRule("com.fasterxml.jackson.module")
        ),
        "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test",
      ),
      Compile / sourceGenerators += Def.task {
        val file = (Compile / sourceManaged).value / "io" / "delta" / "standalone" / "package.scala"
        IO.write(file,
          s"""package io.delta
             |
             |package object standalone {
             |  val VERSION = "${version.value}"
             |  val NAME = "Delta Standalone"
             |}
             |""".stripMargin)
        Seq(file)
      },

      /**
       * Standalone packaged (unshaded) jar.
       *
       * Build with `build/sbt standalone/package` command.
       * e.g. connectors/standalone/target/scala-2.12/delta-standalone-original-unshaded_2.12-0.2.1-SNAPSHOT.jar
       */
      artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
        artifact.name + "-unshaded" + "_" + sv.binary + "-" + module.revision  + "." + artifact.extension
      },

      /**
       * Standalone assembly (shaded) jar. This is what we want to release.
       *
       * Build with `build/sbt standalone/assembly` command.
       * e.g. connectors/standalone/target/scala-2.12/delta-standalone-original-shaded_2.12-0.2.1-SNAPSHOT.jar
       */
      assembly / logLevel := Level.Info,
      assembly / test := {},
      assembly / assemblyJarName := s"${name.value}-shaded_${scalaBinaryVersion.value}-${version.value}.jar",
      // We exclude jars first, and then we shade what is remaining. Note: the input here is only
      // `libraryDependencies` jars, not `.dependsOn(_)` jars.
      assembly / assemblyExcludedJars := {
        val cp = (assembly / fullClasspath).value
        val allowedPrefixes = Set("META_INF", "io", "json4s", "jackson", "paranamer",
          "parquet4s", "parquet-", "audience-annotations", "commons-pool")
        cp.filter { f =>
          !allowedPrefixes.exists(prefix => f.data.getName.startsWith(prefix))
        }
      },
      assembly / assemblyShadeRules := Seq(
        ShadeRule.rename("com.fasterxml.jackson.**" -> "shadedelta.@0").inAll,
        ShadeRule.rename("com.thoughtworks.paranamer.**" -> "shadedelta.@0").inAll,
        ShadeRule.rename("org.json4s.**" -> "shadedelta.@0").inAll,
        ShadeRule.rename("com.github.mjakubowski84.parquet4s.**" -> "shadedelta.@0").inAll,
        ShadeRule.rename("org.apache.commons.pool.**" -> "shadedelta.@0").inAll,
        ShadeRule.rename("org.apache.parquet.**" -> "shadedelta.@0").inAll,
        ShadeRule.rename("shaded.parquet.**" -> "shadedelta.@0").inAll,
        ShadeRule.rename("org.apache.yetus.audience.**" -> "shadedelta.@0").inAll
      ),
      assembly / assemblyMergeStrategy := {
        // Discard `module-info.class` to fix the `different file contents found` error.
        // TODO Upgrade SBT to 1.5 which will do this automatically
        case "module-info.class" => MergeStrategy.discard
        // Discard unused `parquet.thrift` so that we don't conflict the file used by the user
        case "parquet.thrift" => MergeStrategy.discard
        // Discard the jackson service configs that we don't need. These files are not shaded so
        // adding them may conflict with other jackson version used by the user.
        case PathList("META-INF", "services", xs @ _*) => MergeStrategy.discard
        // This project `.dependsOn` delta-storage, and its classes will be included by default
        // in this assembly jar. Manually discard them since it is already a compile-time dependency.
        case PathList("io", "delta", "storage", xs @ _*) => MergeStrategy.discard
        case x =>
          val oldStrategy = (assembly / assemblyMergeStrategy).value
          oldStrategy(x)
      },
      assembly / artifact := {
        val art = (assembly / artifact).value
        art.withClassifier(Some("assembly"))
      },
      addArtifact(assembly / artifact, assembly),

      // Unidoc setting
      unidocSourceFilePatterns += SourceFilePattern("io/delta/standalone/"),
      javaCheckstyleSettings("dev/connectors-checkstyle.xml")
    ).configureUnidoc()

  /** A dummy project to allow `standaloneParquet` depending on the shaded standalone jar. */
  lazy val standaloneWithoutParquetUtils = project
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-standalone-without-parquet-utils",
      commonSettings,
      skipReleaseSettings,
      exportJars := true,
      Compile / packageBin := (standalone / assembly).value
    )

  /**
   * The public API ParquetSchemaConverter exposes Parquet classes in its methods so we cannot apply
   * shading rules on it. However, sbt-assembly doesn't allow excluding a single file. Hence, we
   * create a separate project to skip the shading.
   */
  lazy val standaloneParquet = (project in file("connectors/standalone-parquet"))
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .dependsOn(standaloneWithoutParquetUtils)
    .settings(
      name := "delta-standalone-parquet",
      commonSettings,
      skipReleaseSettings,
      libraryDependencies ++= Seq(
        "org.apache.parquet" % "parquet-hadoop" % "1.12.3" % "provided",
        "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test"
      ),
      assemblyPackageScala / assembleArtifact := false
    )

  /**
   * Cosmetic project used for publishing the standalone shaded JAR.
   * We want to publish the `standalone` project's shaded JAR, but sbt publish uses the
   * non-shaded JAR by default. This project packages the shaded JAR correctly for publishing.
   */
  lazy val standaloneCosmetic = project
    .dependsOn(StorageProjects.storage) // this doesn't impact the output artifact (jar), only the pom.xml dependencies
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-standalone",
      commonSettings,
      releaseSettings,
      exportJars := true,
      Compile / packageBin := (standaloneParquet / assembly).value,
      Compile / packageSrc := (standalone / Compile / packageSrc).value,
      libraryDependencies ++= scalaCollectionPar(scalaVersion.value) ++ Seq(
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
        "org.apache.parquet" % "parquet-hadoop" % "1.12.3" % "provided",
        // parquet4s-core dependencies that are not shaded are added with compile scope.
        "com.chuusai" %% "shapeless" % "2.3.4",
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.3"
      )
    )

  /**
   * Test project for standaloneCosmetic.
   */
  lazy val testStandaloneCosmetic = (project in file("connectors/testStandaloneCosmetic"))
    .dependsOn(standaloneCosmetic)
    .dependsOn(goldenTables % "test")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "test-standalone-cosmetic",
      commonSettings,
      skipReleaseSettings,
      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
        "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test",
      )
    )

  /**
   * A test project to verify `ParquetSchemaConverter` APIs are working after the user provides
   * `parquet-hadoop`. We use a separate project because we want to test whether Delta Standlone APIs
   * except `ParquetSchemaConverter` are working without `parquet-hadoop` in testStandaloneCosmetic`.
   */
  lazy val testParquetUtilsWithStandaloneCosmetic = project.dependsOn(standaloneCosmetic)
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "test-parquet-utils-with-standalone-cosmetic",
      commonSettings,
      skipReleaseSettings,
      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
        "org.apache.parquet" % "parquet-hadoop" % "1.12.3" % "provided",
        "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test",
      )
    )

  /**
   * SQL Delta Import - tool for importing RDBMS data into Delta tables.
   */
  lazy val sqlDeltaImport = (project in file("connectors/sql-delta-import"))
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "sql-delta-import",
      commonSettings,
      skipReleaseSettings,
      publishArtifact := scalaBinaryVersion.value != "2.11",
      Test / publishArtifact := false,
      libraryDependencies ++= Seq(
        // Using released delta-spark JAR instead of module dependency to break circular dependency
        "io.delta" %% "delta-spark" % "3.3.2",

        "io.netty" % "netty-buffer"  % "4.1.63.Final" % "test",
        "org.apache.spark" % ("spark-sql_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % defaultSparkVersion % "provided",
        "org.rogach" %% "scallop" % "3.5.1",
        "org.scalatest" %% "scalatest" % scalaTestVersionForConnectors % "test",
        "com.h2database" % "h2" % "1.4.200" % "test",
        "org.apache.spark" % ("spark-catalyst_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % defaultSparkVersion % "test",
        "org.apache.spark" % ("spark-core_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % defaultSparkVersion % "test",
        "org.apache.spark" % ("spark-sql_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % defaultSparkVersion % "test"
      )
    )
}
