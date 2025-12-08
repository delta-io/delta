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
import ShadedIcebergBuild._

/**
 * Iceberg integration projects - read/write Iceberg tables from Delta.
 */
object IcebergProjects {

  val icebergSparkRuntimeArtifactName = {
    val (expMaj, expMin, _) = getMajorMinorPatch(defaultSparkVersion)
    s"iceberg-spark-runtime-$expMaj.$expMin"
  }

  val deltaIcebergSparkIncludePrefixes = Seq(
    // We want everything from this package
    "org/apache/spark/sql/delta/icebergShaded",

    // We only want the files in this project from this package. e.g. we want to exclude
    // org/apache/spark/sql/delta/commands/convert/ConvertTargetFile.class (from delta-spark project).
    "org/apache/spark/sql/delta/commands/convert/IcebergFileManifest",
    "org/apache/spark/sql/delta/commands/convert/IcebergSchemaUtils",
    "org/apache/spark/sql/delta/commands/convert/IcebergTable"
  )

  val icebergShadedVersion = "1.10.0"

  /**
   * Test project for delta-iceberg JAR.
   */
  lazy val testDeltaIcebergJar = (project in file("testDeltaIcebergJar"))
    // delta-iceberg depends on delta-spark! So, we need to include it during our test.
    .dependsOn(SparkProjects.spark % "test")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "test-delta-iceberg-jar",
      commonSettings,
      skipReleaseSettings,
      exportJars := true,
      Compile / unmanagedJars += (iceberg / assembly).value,
      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "org.apache.spark" %% "spark-core" % defaultSparkVersion % "test"
      )
    )

  /**
   * Shaded Iceberg libraries for Delta.
   */
  lazy val icebergShaded = (project in file("icebergShaded"))
    .dependsOn(SparkProjects.spark % "provided")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "iceberg-shaded",
      commonSettings,
      skipReleaseSettings,
      // must exclude all dependencies from Iceberg that delta-spark includes
      libraryDependencies ++= Seq(
        // Fix Iceberg's legacy java.lang.NoClassDefFoundError: scala/jdk/CollectionConverters$ error
        // due to legacy scala.
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.1" % "provided",
        "org.apache.iceberg" % "iceberg-core" % icebergShadedVersion excludeAll (
          icebergExclusionRules: _*
        ),
        "org.apache.iceberg" % "iceberg-hive-metastore" % icebergShadedVersion excludeAll (
          icebergExclusionRules: _*
        ),
        // the hadoop client and hive metastore versions come from this file in the
        // iceberg repo of icebergShadedVersion: iceberg/gradle/libs.versions.toml
        "org.apache.hadoop" % "hadoop-client" % "2.7.3" % "provided" excludeAll (
          hadoopClientExclusionRules: _*
        ),
        "org.apache.hive" % "hive-metastore" % "2.3.8" % "provided" excludeAll (
          hiveMetastoreExclusionRules: _*
        )
      ),
      // Generated shaded Iceberg JARs
      Compile / packageBin := assembly.value,
      assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar",
      assembly / logLevel := Level.Info,
      assembly / test := {},
      assembly / assemblyShadeRules := Seq(
        ShadeRule.rename("org.apache.iceberg.**" -> "shadedForDelta.@0").inAll
      ),
      assembly / assemblyExcludedJars := {
        val cp = (assembly / fullClasspath).value
        cp.filter { jar =>
          val doExclude = jar.data.getName.contains("jackson-annotations") ||
            jar.data.getName.contains("RoaringBitmap")
          doExclude
        }
      },
      // all following clases have Delta customized implementation under icebergShaded/src and thus
      // require them to be 'first' to replace the class from iceberg jar
      assembly / assemblyMergeStrategy := updateMergeStrategy((assembly / assemblyMergeStrategy).value),
      assemblyPackageScala / assembleArtifact := false,
    )

  // Build using: build/sbt clean icebergShaded/compile iceberg/compile
  // It will fail the first time, just re-run it.
  // scalastyle:off println
  /**
   * Delta Iceberg - connector for reading/writing Iceberg tables.
   */
  lazy val iceberg = (project in file("iceberg"))
    .dependsOn(SparkProjects.spark % "compile->compile;test->test;provided->provided")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-iceberg",
      commonSettings,
      scalaStyleSettings,
      releaseSettings,
      CrossSparkVersions.sparkDependentModuleName(sparkVersion),
      libraryDependencies ++= Seq(
        // Fix Iceberg's legacy java.lang.NoClassDefFoundError: scala/jdk/CollectionConverters$ error
        // due to legacy scala.
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.1",
        "org.apache.iceberg" %% icebergSparkRuntimeArtifactName % "1.4.0" % "provided",
        "com.github.ben-manes.caffeine" % "caffeine" % "2.9.3"
      ),
      Compile / unmanagedJars += (icebergShaded / assembly).value,
      // Generate the assembly JAR as the package JAR
      Compile / packageBin := assembly.value,
      assembly / assemblyJarName := {
        s"${moduleName.value}_${scalaBinaryVersion.value}-${version.value}.jar"
      },
      assembly / logLevel := Level.Info,
      assembly / test := {},
      assembly / assemblyExcludedJars := {
        // Note: the input here is only `libraryDependencies` jars, not `.dependsOn(_)` jars.
        val allowedJars = Seq(
          s"iceberg-shaded_${scalaBinaryVersion.value}-${version.value}.jar",
          s"scala-library-${scala213}.jar",
          s"scala-collection-compat_${scalaBinaryVersion.value}-2.1.1.jar",
          "caffeine-2.9.3.jar",
          // Note: We are excluding
          // - antlr4-runtime-4.9.3.jar
          // - checker-qual-3.19.0.jar
          // - error_prone_annotations-2.10.0.jar
        )
        val cp = (assembly / fullClasspath).value

        // Return `true` when we want the jar `f` to be excluded from the assembly jar
        cp.filter { f =>
          val doExclude = !allowedJars.contains(f.data.getName)
          println(s"Excluding jar: ${f.data.getName} ? $doExclude")
          doExclude
        }
      },
      assembly / assemblyMergeStrategy := {
        // Project iceberg `dependsOn` spark and accidentally brings in it, along with its
        // compile-time dependencies (like delta-storage). We want these excluded from the
        // delta-iceberg jar.
        case PathList("io", "delta", xs @ _*) =>
          // - delta-storage will bring in classes: io/delta/storage
          // - delta-spark will bring in classes: io/delta/exceptions/, io/delta/implicits,
          //   io/delta/package, io/delta/sql, io/delta/tables,
          MergeStrategy.discard
        case PathList("com", "databricks", xs @ _*) =>
          // delta-spark will bring in com/databricks/spark/util
          MergeStrategy.discard
        case PathList("org", "apache", "spark", xs @ _*)
          if !deltaIcebergSparkIncludePrefixes.exists { prefix =>
            s"org/apache/spark/${xs.mkString("/")}".startsWith(prefix) } =>
          MergeStrategy.discard
        case PathList("scoverage", xs @ _*) =>
          MergeStrategy.discard
        case x =>
          (assembly / assemblyMergeStrategy).value(x)
      },
      assemblyPackageScala / assembleArtifact := false
    )
  // scalastyle:on println

  /**
   * Aggregator for iceberg projects.
   */
  lazy val icebergGroup = project
    .aggregate(iceberg, testDeltaIcebergJar)
    .settings(
      // crossScalaVersions must be set to Nil on the aggregating project
      crossScalaVersions := Nil,
      publishArtifact := false,
      publish / skip := false,
    )
}
