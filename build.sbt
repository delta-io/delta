/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

// scalastyle:off line.size.limit

import ReleaseTransformations._
import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq, _}
import scala.xml.transform._

// Disable parallel execution to workaround https://github.com/etsy/sbt-checkstyle-plugin/issues/32
concurrentRestrictions in Global := {
  Tags.limitAll(1) :: Nil
}

inThisBuild(
  Seq(
    parallelExecution := false,
  )
)

// crossScalaVersions must be set to Nil on the root project
crossScalaVersions := Nil
val scala213 = "2.13.8"
val scala212 = "2.12.8"
val scala211 = "2.11.12"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

val sparkVersion = "2.4.3"
val hiveDeltaVersion = "0.5.0"
val parquet4sVersion = "1.9.4"
val scalaTestVersion = "3.0.8"
val deltaStorageVersion = "2.0.0"
// Versions for Hive 3
val hadoopVersion = "3.1.0"
val hiveVersion = "3.1.2"
val tezVersion = "0.9.2"
// Versions for Hive 2
val hadoopVersionForHive2 = "2.7.2"
val hive2Version = "2.3.3"
val tezVersionForHive2 = "0.8.4"

def scalacWarningUnusedImport(version: String) = version match {
    case v if v.startsWith("2.13.") => "-Ywarn-unused:imports"
    case _ => "-Ywarn-unused-import"
}

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := scala212,
  crossScalaVersions := Seq(scala213, scala212, scala211),
  fork := true,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  scalacOptions ++= Seq("-target:jvm-1.8", scalacWarningUnusedImport(scalaVersion.value) ),
  // Configurations to speed up tests and reduce memory footprint
  Test / javaOptions ++= Seq(
    "-Dspark.ui.enabled=false",
    "-Dspark.ui.showConsoleProgress=false",
    "-Dspark.databricks.delta.snapshotPartitions=2",
    "-Dspark.sql.shuffle.partitions=5",
    "-Ddelta.log.cacheSize=3",
    "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
    "-Xmx1024m"
  ),
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  (Compile / compile ) := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  (Test / test) := ((Test / test) dependsOn testScalastyle).value,

  // Can be run explicitly via: build/sbt $module/checkstyle
  // Will automatically be run during compilation (e.g. build/sbt compile)
  // and during tests (e.g. build/sbt test)
  checkstyleConfigLocation := CheckstyleConfigLocation.File("dev/checkstyle.xml"),
  checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error),
  (Compile / checkstyle) := (Compile / checkstyle).triggeredBy(Compile / compile).value,
  (Test / checkstyle) := (Test / checkstyle).triggeredBy(Test / compile).value
)

lazy val releaseSettings = Seq(
  publishMavenStyle := true,
  publishArtifact := true,
  Test / publishArtifact := false,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseCrossBuild := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) {
      Some("snapshots" at nexus + "content/repositories/snapshots")
    } else {
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  },
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  pomExtra :=
    <url>https://github.com/delta-io/connectors</url>
      <scm>
        <url>git@github.com:delta-io/connectors.git</url>
        <connection>scm:git:git@github.com:delta-io/connectors.git</connection>
      </scm>
      <developers>
        <developer>
          <id>tdas</id>
          <name>Tathagata Das</name>
          <url>https://github.com/tdas</url>
        </developer>
        <developer>
          <id>scottsand-db</id>
          <name>Scott Sandre</name>
          <url>https://github.com/scottsand-db</url>
        </developer>
        <developer>
          <id>windpiger</id>
          <name>Jun Song</name>
          <url>https://github.com/windpiger</url>
        </developer>
        <developer>
          <id>zsxwing</id>
          <name>Shixiong Zhu</name>
          <url>https://github.com/zsxwing</url>
        </developer>
      </developers>
)

lazy val skipReleaseSettings = Seq(
  publishArtifact := false,
  publish / skip := true
)

// Looks some of release settings should be set for the root project as well.
publishArtifact := false  // Don't release the root project
publish / skip := true
publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
releaseCrossBuild := false
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  setNextVersion,
  commitNextVersion
)

lazy val hive = (project in file("hive")) dependsOn(standaloneCosmetic) settings (
  name := "delta-hive",
  commonSettings,
  releaseSettings,

  // Minimal dependencies to compile the codes. This project doesn't run any tests so we don't need
  // any runtime dependencies.
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" classifier "core",
    "org.apache.hive" % "hive-metastore" % hiveVersion % "provided"
  )
)

lazy val hiveAssembly = (project in file("hive-assembly")) dependsOn(hive) settings(
  name := "delta-hive-assembly",
  Compile / unmanagedJars += (hive / assembly).value,
  commonSettings,
  skipReleaseSettings,

  assembly / logLevel := Level.Info,
  assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar",
  assembly / test := {},
  // Make the 'compile' invoke the 'assembly' task to generate the uber jar.
  Compile / packageBin := assembly.value
)

lazy val hiveTest = (project in file("hive-test")) settings (
  name := "hive-test",
  // Make the project use the assembly jar to ensure we are testing the assembly jar that users will
  // use in real environment.
  Compile / unmanagedJars += (hiveAssembly / Compile / packageBin / packageBin).value,
  commonSettings,
  skipReleaseSettings,
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" classifier "core" excludeAll(
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.apache.hive" % "hive-metastore" % hiveVersion % "provided"  excludeAll(
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.apache.hive", "hive-exec")
    ),
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule("org.apache.hive", "hive-exec"),
      ExclusionRule("com.google.guava", "guava"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  )
)

lazy val hiveMR = (project in file("hive-mr")) dependsOn(hiveTest % "test->test") settings (
  name := "hive-mr",
  commonSettings,
  skipReleaseSettings,
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" excludeAll(
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("com.google.guava", "guava"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  )
)

lazy val hiveTez = (project in file("hive-tez")) dependsOn(hiveTest % "test->test") settings (
  name := "hive-tez",
  commonSettings,
  skipReleaseSettings,
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided" excludeAll (
      ExclusionRule(organization = "com.google.protobuf")
      ),
    "com.google.protobuf" % "protobuf-java" % "2.5.0",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" classifier "core" excludeAll(
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.jodd" % "jodd-core" % "3.5.2",
    "org.apache.hive" % "hive-metastore" % hiveVersion % "provided" excludeAll(
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.apache.hive", "hive-exec")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule("org.apache.hive", "hive-exec"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion % "test",
    "org.apache.tez" % "tez-mapreduce" % tezVersion % "test",
    "org.apache.tez" % "tez-dag" % tezVersion % "test",
    "org.apache.tez" % "tez-tests" % tezVersion % "test" classifier "tests",
    "com.esotericsoftware" % "kryo-shaded" % "4.0.2" % "test",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  )
)


lazy val hive2MR = (project in file("hive2-mr")) settings (
  name := "hive2-mr",
  commonSettings,
  skipReleaseSettings,
  Compile / unmanagedJars ++= Seq(
    (hiveAssembly / Compile / packageBin / packageBin).value,
    (hiveTest / Test / packageBin / packageBin).value
  ),
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersionForHive2 % "provided",
    "org.apache.hive" % "hive-exec" % hive2Version % "provided" excludeAll(
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersionForHive2 % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hive2Version % "test" excludeAll(
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("com.google.guava", "guava"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  )
)

lazy val hive2Tez = (project in file("hive2-tez")) settings (
  name := "hive2-tez",
  commonSettings,
  skipReleaseSettings,
  Compile / unmanagedJars ++= Seq(
    (hiveAssembly / Compile / packageBin / packageBin).value,
    (hiveTest / Test / packageBin / packageBin).value
  ),
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersionForHive2 % "provided" excludeAll (
      ExclusionRule(organization = "com.google.protobuf")
      ),
    "com.google.protobuf" % "protobuf-java" % "2.5.0",
    "org.apache.hive" % "hive-exec" % hive2Version % "provided" classifier "core" excludeAll(
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.jodd" % "jodd-core" % "3.5.2",
    "org.apache.hive" % "hive-metastore" % hive2Version % "provided" excludeAll(
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.apache.hive", "hive-exec")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersionForHive2 % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hive2Version % "test" excludeAll(
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule("org.apache.hive", "hive-exec"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersionForHive2 % "test",
    "org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersionForHive2 % "test",
    "org.apache.tez" % "tez-mapreduce" % tezVersionForHive2 % "test",
    "org.apache.tez" % "tez-dag" % tezVersionForHive2 % "test",
    "org.apache.tez" % "tez-tests" % tezVersionForHive2 % "test" classifier "tests",
    "com.esotericsoftware" % "kryo-shaded" % "4.0.2" % "test",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  )
)

/**
 * We want to publish the `standalone` project's shaded JAR (created from the
 * build/sbt standalone/assembly command).
 *
 * However, build/sbt standalone/publish and build/sbt standalone/publishLocal will use the
 * non-shaded JAR from the build/sbt standalone/package command.
 *
 * So, we create an impostor, cosmetic project used only for publishing.
 *
 * build/sbt standaloneCosmetic/package
 * - creates connectors/standalone/target/scala-2.12/delta-standalone-original-shaded_2.12-0.2.1-SNAPSHOT.jar
 *   (this is the shaded JAR we want)
 *
 * build/sbt standaloneCosmetic/publishM2
 * - packages the shaded JAR (above) and then produces:
 * -- .m2/repository/io/delta/delta-standalone_2.12/0.2.1-SNAPSHOT/delta-standalone_2.12-0.2.1-SNAPSHOT.pom
 * -- .m2/repository/io/delta/delta-standalone_2.12/0.2.1-SNAPSHOT/delta-standalone_2.12-0.2.1-SNAPSHOT.jar
 * -- .m2/repository/io/delta/delta-standalone_2.12/0.2.1-SNAPSHOT/delta-standalone_2.12-0.2.1-SNAPSHOT-sources.jar
 * -- .m2/repository/io/delta/delta-standalone_2.12/0.2.1-SNAPSHOT/delta-standalone_2.12-0.2.1-SNAPSHOT-javadoc.jar
 */
lazy val standaloneCosmetic = project
  .settings(
    name := "delta-standalone",
    commonSettings,
    releaseSettings,
    exportJars := true,
    Compile / packageBin := (standaloneParquet / assembly).value,
    libraryDependencies ++= scalaCollectionPar(scalaVersion.value) ++ Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.parquet" % "parquet-hadoop" % "1.12.0" % "provided",
      "io.delta" % "delta-storage" % deltaStorageVersion,
      // parquet4s-core dependencies that are not shaded are added with compile scope.
      "com.chuusai" %% "shapeless" % "2.3.4",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.3"
    )
  )

lazy val testStandaloneCosmetic = project.dependsOn(standaloneCosmetic)
  .settings(
    name := "test-standalone-cosmetic",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    )
  )

/**
 * A test project to verify `ParquetSchemaConverter` APIs are working after the user provides
 * `parquet-hadoop`. We use a separate project because we want to test whether Delta Standlone APIs
 * except `ParquetSchemaConverter` are working without `parquet-hadoop` in testStandaloneCosmetic`.
 */
lazy val testParquetUtilsWithStandaloneCosmetic = project.dependsOn(standaloneCosmetic)
  .settings(
    name := "test-parquet-utils-with-standalone-cosmetic",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.parquet" % "parquet-hadoop" % "1.12.0" % "provided",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    )
  )

def scalaCollectionPar(version: String) = version match {
  case v if v.startsWith("2.13.") =>
    Seq("org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4")
    case _ => Seq()
}

/**
 * The public API ParquetSchemaConverter exposes Parquet classes in its methods so we cannot apply
 * shading rules on it. However, sbt-assembly doesn't allow excluding a single file. Hence, we
 * create a separate project to skip the shading.
 */
lazy val standaloneParquet = (project in file("standalone-parquet"))
  .dependsOn(standaloneWithoutParquetUtils)
  .settings(
    name := "delta-standalone-parquet",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "org.apache.parquet" % "parquet-hadoop" % "1.12.0" % "provided",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ),
    assemblyPackageScala / assembleArtifact := false
  )

/** A dummy project to allow `standaloneParquet` depending on the shaded standalone jar. */
lazy val standaloneWithoutParquetUtils = project
  .settings(
    name := "delta-standalone-without-parquet-utils",
    commonSettings,
    skipReleaseSettings,
    exportJars := true,
    Compile / packageBin := (standalone / assembly).value
  )

lazy val standalone = (project in file("standalone"))
  .enablePlugins(GenJavadocPlugin, JavaUnidocPlugin)
  .settings(
    name := "delta-standalone-original",
    commonSettings,
    skipReleaseSettings,
    mimaSettings, // TODO(scott): move this to standaloneCosmetic
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
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "io.delta" % "delta-storage" % deltaStorageVersion,

      // Compiler plugins
      // -- Bump up the genjavadoc version explicitly to 0.18 to work with Scala 2.12
      compilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.18" cross CrossVersion.full)
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
    // we exclude jars first, and then we shade what is remaining
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
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / artifact := {
      val art = (assembly / artifact).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(assembly / artifact, assembly),
    /**
     * Unidoc settings
     * Generate javadoc with `unidoc` command, outputs to `standalone/target/javaunidoc`
     */
    JavaUnidoc / unidoc / javacOptions := Seq(
      "-public",
      "-windowtitle", "Delta Standalone " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
      "-noqualifier", "java.lang",
      "-tag", "implNote:a:Implementation Note:",
      "-Xdoclint:all"
    ),
    JavaUnidoc / unidoc /  unidocAllSources := {
      (JavaUnidoc / unidoc / unidocAllSources).value
        // ignore any internal Scala code
        .map(_.filterNot(_.getName.contains("$")))
        .map(_.filterNot(_.getCanonicalPath.contains("/internal/")))
        // ignore project `hive` which depends on this project
        .map(_.filterNot(_.getCanonicalPath.contains("/hive/")))
        // ignore project `flink` which depends on this project
        .map(_.filterNot(_.getCanonicalPath.contains("/flink/")))
    },
    // Ensure unidoc is run with tests. Must be cleaned before test for unidoc to be generated.
    (Test / test) := ((Test / test) dependsOn (Compile / unidoc)).value
  )

/*
 ********************
 *  MIMA settings   *
 ********************
 */
def getPrevVersion(currentVersion: String): String = {
  implicit def extractInt(str: String): Int = {
    """\d+""".r.findFirstIn(str).map(java.lang.Integer.parseInt).getOrElse {
      throw new Exception(s"Could not extract version number from $str in $version")
    }
  }

  val (major, minor, patch): (Int, Int, Int) = {
    currentVersion.split("\\.").toList match {
      case majorStr :: minorStr :: patchStr :: _ =>
        (majorStr, minorStr, patchStr)
      case _ => throw new Exception(s"Could not find previous version for $version.")
    }
  }

  val majorToLastMinorVersions: Map[Int, Int] = Map(
    // TODO add mapping when required
    // e.g. 0 -> 8
  )
  if (minor == 0) {  // 1.0.0
    val prevMinor = majorToLastMinorVersions.getOrElse(major - 1, {
      throw new Exception(s"Last minor version of ${major - 1}.x.x not configured.")
    })
    s"${major - 1}.$prevMinor.0"  // 1.0.0 -> 0.8.0
  } else if (patch == 0) {
    s"$major.${minor - 1}.0"      // 1.1.0 -> 1.0.0
  } else {
    s"$major.$minor.${patch - 1}" // 1.1.1 -> 1.1.0
  }
}

lazy val mimaSettings = Seq(
  (Test / test) := ((Test / test) dependsOn mimaReportBinaryIssues).value,
  mimaPreviousArtifacts := {
    if (CrossVersion.partialVersion(scalaVersion.value) == Some((2, 13))) {
      // Skip mima check since we don't have a Scala 2.13 release yet.
      // TODO Update this after releasing 0.4.0.
      Set.empty
    } else {
      Set("io.delta" %% "delta-standalone" % getPrevVersion(version.value))
    }
  },
  mimaBinaryIssueFilters ++= StandaloneMimaExcludes.ignoredABIProblems
)

lazy val compatibility = (project in file("oss-compatibility-tests"))
  // depend on standalone test codes as well
  .dependsOn(standalone % "compile->compile;test->test")
  .settings(
    name := "compatibility",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      // Test Dependencies
      "io.netty" % "netty-buffer"  % "4.1.63.Final" % "test",
      "org.scalatest" %% "scalatest" % "3.1.0" % "test",
      "org.apache.spark" % "spark-sql_2.12" % "3.2.0" % "test",
      "io.delta" % "delta-core_2.12" % "1.1.0" % "test",
      "commons-io" % "commons-io" % "2.8.0" % "test",
      "org.apache.spark" % "spark-catalyst_2.12" % "3.2.0" % "test" classifier "tests",
      "org.apache.spark" % "spark-core_2.12" % "3.2.0" % "test" classifier "tests",
      "org.apache.spark" % "spark-sql_2.12" % "3.2.0" % "test" classifier "tests",
    )
  )

lazy val goldenTables = (project in file("golden-tables")) settings (
  name := "golden-tables",
  commonSettings,
  skipReleaseSettings,
  libraryDependencies ++= Seq(
    // Test Dependencies
    "org.scalatest" %% "scalatest" % "3.1.0" % "test",
    "org.apache.spark" % "spark-sql_2.12" % "3.2.0" % "test",
    "io.delta" % "delta-core_2.12" % "1.1.0" % "test",
    "commons-io" % "commons-io" % "2.8.0" % "test",
    "org.apache.spark" % "spark-catalyst_2.12" % "3.2.0" % "test" classifier "tests",
    "org.apache.spark" % "spark-core_2.12" % "3.2.0" % "test" classifier "tests",
    "org.apache.spark" % "spark-sql_2.12" % "3.2.0" % "test" classifier "tests"
  )
)

def sqlDeltaImportScalaVersion(scalaBinaryVersion: String): String = {
  scalaBinaryVersion match {
    // sqlDeltaImport doesn't support 2.11. We return 2.12 so that we can resolve the dependencies
    // but we will not publish sqlDeltaImport with Scala 2.11.
    case "2.11" => "2.12"
    case _ => scalaBinaryVersion
  }
}

lazy val sqlDeltaImport = (project in file("sql-delta-import"))
  .settings (
    name := "sql-delta-import",
    commonSettings,
    skipReleaseSettings,
    publishArtifact := scalaBinaryVersion.value != "2.11",
    Test / publishArtifact := false,
    libraryDependencies ++= Seq(
      "io.netty" % "netty-buffer"  % "4.1.63.Final" % "test",
      "org.apache.spark" % ("spark-sql_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % "3.2.0" % "provided",
      "io.delta" % ("delta-core_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % "1.1.0" % "provided",
      "org.rogach" %% "scallop" % "3.5.1",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "com.h2database" % "h2" % "1.4.200" % "test",
      "org.apache.spark" % ("spark-catalyst_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % "3.2.0" % "test",
      "org.apache.spark" % ("spark-core_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % "3.2.0" % "test",
      "org.apache.spark" % ("spark-sql_" + sqlDeltaImportScalaVersion(scalaBinaryVersion.value)) % "3.2.0" % "test"
    )
  )

def flinkScalaVersion(scalaBinaryVersion: String): String = {
  scalaBinaryVersion match {
    // Flink doesn't support 2.13. We return 2.12 so that we can resolve the dependencies but we
    // will not publish Flink connector with Scala 2.13.
    case "2.13" => "2.12"
    case _ => scalaBinaryVersion
  }
}

val flinkVersion = "1.13.0"
lazy val flink = (project in file("flink"))
  .dependsOn(standaloneCosmetic % "provided")
  .enablePlugins(GenJavadocPlugin, JavaUnidocPlugin)
  .settings (
    name := "delta-flink",
    commonSettings,
    releaseSettings,
    publishArtifact := scalaBinaryVersion.value == "2.12", // only publish once
    autoScalaLibrary := false, // exclude scala-library from dependencies
    Test / publishArtifact := false,
    pomExtra :=
      <url>https://github.com/delta-io/connectors</url>
        <scm>
          <url>git@github.com:delta-io/connectors.git</url>
          <connection>scm:git:git@github.com:delta-io/connectors.git</connection>
        </scm>
        <developers>
          <developer>
            <id>pkubit-g</id>
            <name>Paweł Kubit</name>
            <url>https://github.com/pkubit-g</url>
          </developer>
          <developer>
            <id>kristoffSC</id>
            <name>Krzysztof Chmielewski</name>
            <url>https://github.com/kristoffSC</url>
          </developer>
        </developers>,
    crossPaths := false,
    libraryDependencies ++= Seq(
      "org.apache.flink" % ("flink-parquet_" + flinkScalaVersion(scalaBinaryVersion.value)) % flinkVersion % "provided",
      "org.apache.flink" % "flink-table-common" % flinkVersion % "provided",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.flink" % "flink-connector-files" % flinkVersion % "test" classifier "tests",
      "org.apache.flink" % ("flink-table-runtime-blink_" + flinkScalaVersion(scalaBinaryVersion.value)) % flinkVersion % "provided",
      "org.apache.flink" % "flink-connector-test-utils" % flinkVersion % "test",
      "org.apache.flink" % ("flink-clients_" + flinkScalaVersion(scalaBinaryVersion.value)) % flinkVersion % "test",
      "org.apache.flink" % ("flink-test-utils_" + flinkScalaVersion(scalaBinaryVersion.value)) % flinkVersion % "test",
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
      "org.mockito" % "mockito-inline" % "3.8.0" % "test",
      "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
      "org.junit.vintage" % "junit-vintage-engine" % "5.8.2" % "test",
      "org.mockito" % "mockito-junit-jupiter" % "4.5.0" % "test",
      "org.junit.jupiter" % "junit-jupiter-params" % "5.8.2" % "test",
      "io.github.artsok" % "rerunner-jupiter" % "2.1.6" % "test",

      // Compiler plugins
      // -- Bump up the genjavadoc version explicitly to 0.18 to work with Scala 2.12
      compilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.18" cross CrossVersion.full)
    ),
    // generating source java class with version number to be passed during commit to the DeltaLog as engine info
    // (part of transaction's metadata)
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "meta" / "Meta.java"
      IO.write(file,
        s"""package io.delta.flink.sink.internal.committer;
           |
           |final class Meta {
           |  public static final String FLINK_VERSION = "${flinkVersion}";
           |  public static final String CONNECTOR_VERSION = "${version.value}";
           |}
           |""".stripMargin)
      Seq(file)
    },
    /**
     * Unidoc settings
     * Generate javadoc with `unidoc` command, outputs to `flink/target/javaunidoc`
     * e.g. build/sbt flink/unidoc
     */
    JavaUnidoc / unidoc / javacOptions := Seq(
      "-public",
      "-windowtitle", "Flink/Delta Connector " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
      "-noqualifier", "java.lang",
      "-tag", "implNote:a:Implementation Note:",
      "-tag", "apiNote:a:API Note:",
      "-Xdoclint:all"
    ),
    Compile / doc / javacOptions := (JavaUnidoc / unidoc / javacOptions).value,
    JavaUnidoc / unidoc /  unidocAllSources := {
      (JavaUnidoc / unidoc / unidocAllSources).value
        // include only relevant delta-flink classes
        .map(_.filter(_.getCanonicalPath.contains("/flink/")))
        // exclude internal classes
        .map(_.filterNot(_.getCanonicalPath.contains("/internal/")))
        // exclude flink package
        .map(_.filterNot(_.getCanonicalPath.contains("org/apache/flink/")))
    },
    // Ensure unidoc is run with tests. Must be cleaned before test for unidoc to be generated.
    (Test / test) := ((Test / test) dependsOn (Compile / unidoc)).value
  )
