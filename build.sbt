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

parallelExecution in ThisBuild := false
scalastyleConfig in ThisBuild := baseDirectory.value / "scalastyle-config.xml"
crossScalaVersions in ThisBuild := Seq("2.12.8", "2.11.12")

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

val sparkVersion = "2.4.3"
val hiveDeltaVersion = "0.5.0"
val parquet4sVersion = "1.2.1"
val parquetHadoopVersion = "1.10.1"
val scalaTestVersion = "3.0.5"
// Versions for Hive 3
val hadoopVersion = "3.1.0"
val hiveVersion = "3.1.2"
val tezVersion = "0.9.2"
// Versions for Hive 2
val hadoopVersionForHive2 = "2.7.2"
val hive2Version = "2.3.3"
val tezVersionForHive2 = "0.8.4"

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := "2.12.8",
  fork := true,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-Ywarn-unused-import"),
  // Configurations to speed up tests and reduce memory footprint
  javaOptions in Test ++= Seq(
    "-Dspark.ui.enabled=false",
    "-Dspark.ui.showConsoleProgress=false",
    "-Dspark.databricks.delta.snapshotPartitions=2",
    "-Dspark.sql.shuffle.partitions=5",
    "-Ddelta.log.cacheSize=3",
    "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
    "-Xmx1024m"
  ),
  compileScalastyle := scalastyle.in(Compile).toTask("").value,
  (compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value,
  testScalastyle := scalastyle.in(Test).toTask("").value,
  (test in Test) := ((test in Test) dependsOn testScalastyle).value,

  // Can be run explicitly via: build/sbt $module/checkstyle
  // Will automatically be run during compilation (e.g. build/sbt compile)
  // and during tests (e.g. build/sbt test)
  checkstyleConfigLocation := CheckstyleConfigLocation.File("dev/checkstyle.xml"),
  checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error),
  (checkstyle in Compile) := (checkstyle in Compile).triggeredBy(compile in Compile).value,
  (checkstyle in Test) := (checkstyle in Test).triggeredBy(compile in Test).value
)

lazy val releaseSettings = Seq(
  publishMavenStyle := true,
  releaseCrossBuild := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) {
      Some("snapshots" at nexus + "content/repositories/snapshots")
    } else {
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  },
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
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
      </developers>,
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseStepCommandAndRemaining("+publishLocalSigned"),
    setNextVersion,
    commitNextVersion
  )
)

lazy val skipReleaseSettings = Seq(
  publishArtifact := false,
  publish := ()
)

// Looks some of release settings should be set for the root project as well.
publishArtifact := false  // Don't release the root project
publish := {}
publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
releaseCrossBuild := false

lazy val hive = (project in file("hive")) dependsOn(standaloneCosmetic) settings (
  name := "delta-hive",
  commonSettings,
  releaseSettings,

  // Minimal dependencies to compile the codes. This project doesn't run any tests so we don't need
  // any runtime dependencies.
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.parquet" % "parquet-hadoop" % "1.10.1" % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" classifier "core",
    "org.apache.hive" % "hive-metastore" % hiveVersion % "provided"
  )
)

lazy val hiveAssembly = (project in file("hive-assembly")) dependsOn(hive) settings(
  name := "hive-assembly",
  unmanagedJars in Compile += (assembly in hive).value,
  commonSettings,
  skipReleaseSettings,

  logLevel in assembly := Level.Info,
  assemblyJarName in assembly := s"${name.value}-assembly_${scalaBinaryVersion.value}-${version.value}.jar",
  test in assembly := {},
  // Make the 'compile' invoke the 'assembly' task to generate the uber jar.
  packageBin in Compile := assembly.value
)

lazy val hiveTest = (project in file("hive-test")) settings (
  name := "hive-test",
  // Make the project use the assembly jar to ensure we are testing the assembly jar that users will
  // use in real environment.
  unmanagedJars in Compile += (packageBin in(hiveAssembly, Compile, packageBin)).value,
  commonSettings,
  skipReleaseSettings,
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.parquet" % "parquet-hadoop" % "1.10.1" % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" classifier "core" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.apache.hive" % "hive-metastore" % hiveVersion % "provided"  excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.apache.hive", "hive-exec")
    ),
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule("org.apache.hive", "hive-exec"),
      ExclusionRule("com.google.guava", "guava"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
)

lazy val hiveMR = (project in file("hive-mr")) dependsOn(hiveTest % "test->test") settings (
  name := "hive-mr",
  commonSettings,
  skipReleaseSettings,
  unmanagedResourceDirectories in Test += file("golden-tables/src/test/resources"),
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("com.google.guava", "guava"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
)

lazy val hiveTez = (project in file("hive-tez")) dependsOn(hiveTest % "test->test") settings (
  name := "hive-tez",
  commonSettings,
  skipReleaseSettings,
  unmanagedResourceDirectories in Test += file("golden-tables/src/test/resources"),
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided" excludeAll (
      ExclusionRule(organization = "com.google.protobuf")
      ),
    "org.apache.parquet" % "parquet-hadoop" % "1.10.1" excludeAll(
      ExclusionRule("org.apache.hadoop", "hadoop-client")
      ),
    "com.google.protobuf" % "protobuf-java" % "2.5.0",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" classifier "core" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.jodd" % "jodd-core" % "3.5.2",
    "org.apache.hive" % "hive-metastore" % hiveVersion % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.apache.hive", "hive-exec")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
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
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
)


lazy val hive2MR = (project in file("hive2-mr")) settings (
  name := "hive2-mr",
  commonSettings,
  skipReleaseSettings,
  unmanagedResourceDirectories in Test += file("golden-tables/src/test/resources"),
  unmanagedJars in Compile ++= Seq(
    (packageBin in(hiveAssembly, Compile, packageBin)).value,
    (packageBin in(hiveTest, Test, packageBin)).value
  ),
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersionForHive2 % "provided",
    "org.apache.hive" % "hive-exec" % hive2Version % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersionForHive2 % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hive2Version % "test" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("com.google.guava", "guava"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
)

lazy val hive2Tez = (project in file("hive2-tez")) settings (
  name := "hive2-tez",
  commonSettings,
  skipReleaseSettings,
  unmanagedResourceDirectories in Test += file("golden-tables/src/test/resources"),
  unmanagedJars in Compile ++= Seq(
    (packageBin in(hiveAssembly, Compile, packageBin)).value,
    (packageBin in(hiveTest, Test, packageBin)).value
  ),
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersionForHive2 % "provided" excludeAll (
      ExclusionRule(organization = "com.google.protobuf")
      ),
    "org.apache.parquet" % "parquet-hadoop" % "1.10.1" excludeAll(
      ExclusionRule("org.apache.hadoop", "hadoop-client")
      ),
    "com.google.protobuf" % "protobuf-java" % "2.5.0",
    "org.apache.hive" % "hive-exec" % hive2Version % "provided" classifier "core" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.jodd" % "jodd-core" % "3.5.2",
    "org.apache.hive" % "hive-metastore" % hive2Version % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.apache.hive", "hive-exec")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersionForHive2 % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersionForHive2 % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hive2Version % "test" excludeAll(
      ExclusionRule(organization = "org.apache.parquet"),
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
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
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
    packageBin in Compile := (assembly in standalone).value,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.parquet" % "parquet-hadoop" % parquetHadoopVersion % "provided",
      "com.github.mjakubowski84" %% "parquet4s-core" % parquet4sVersion excludeAll (
        ExclusionRule("org.slf4j", "slf4j-api"),
        ExclusionRule("org.apache.parquet", "parquet-hadoop")
      )
    )
  )

lazy val testStandaloneCosmetic = project.dependsOn(standaloneCosmetic)
  .settings(
    name := "test-standalone-cosmetic",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    )
  )

lazy val standalone = (project in file("standalone"))
  .enablePlugins(GenJavadocPlugin, JavaUnidocPlugin)
  .settings(
    name := "delta-standalone-original",
    skip in publish := true,
    commonSettings,
    skipReleaseSettings,
    mimaSettings,
    unmanagedResourceDirectories in Test += file("golden-tables/src/test/resources"),
    // When updating any dependency here, we should also review `pomPostProcess` in project
    // `standaloneCosmetic` and update it accordingly.
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.parquet" % "parquet-hadoop" % parquetHadoopVersion % "provided",
      "com.github.mjakubowski84" %% "parquet4s-core" % parquet4sVersion excludeAll (
        ExclusionRule("org.slf4j", "slf4j-api"),
        ExclusionRule("org.apache.parquet", "parquet-hadoop")
      ),
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0",
      "org.json4s" %% "json4s-jackson" % "3.7.0-M5" excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ),
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.slf4j" % "slf4j-api" % "1.7.25",
      "org.slf4j" % "slf4j-log4j12" % "1.7.25",

      // Compiler plugins
      // -- Bump up the genjavadoc version explicitly to 0.18 to work with Scala 2.12
      compilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.18" cross CrossVersion.full)
    ),
    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "io" / "delta" / "standalone" / "package.scala"
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
    logLevel in assembly := Level.Info,
    test in assembly := {},
    assemblyJarName in assembly := s"${name.value}-shaded_${scalaBinaryVersion.value}-${version.value}.jar",
    // we exclude jars first, and then we shade what is remaining
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      val allowedPrefixes = Set("META_INF", "io", "json4s", "jackson", "paranamer")
      cp.filter { f =>
        !allowedPrefixes.exists(prefix => f.data.getName.startsWith(prefix))
      }
    },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.fasterxml.jackson.**" -> "shadedelta.@0").inAll,
      ShadeRule.rename("com.thoughtworks.paranamer.**" -> "shadedelta.@0").inAll,
      ShadeRule.rename("org.json4s.**" -> "shadedelta.@0").inAll
    ),
    assemblyMergeStrategy in assembly := {
      // Discard `module-info.class` to fix the `different file contents found` error.
      // TODO Upgrade SBT to 1.5 which will do this automatically
      case "module-info.class" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    artifact in assembly := {
      val art = (artifact in assembly).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(artifact in assembly, assembly),
    /**
     * Unidoc settings
     * Generate javadoc with `unidoc` command, outputs to `standalone/target/javaunidoc`
     */
    javacOptions in (JavaUnidoc, unidoc) := Seq(
      "-public",
      "-windowtitle", "Delta Standalone Reader " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
      "-noqualifier", "java.lang",
      "-tag", "implNote:a:Implementation Note:",
      "-Xdoclint:all"
    ),
    unidocAllSources in(JavaUnidoc, unidoc) := {
      (unidocAllSources in(JavaUnidoc, unidoc)).value
        // ignore any internal Scala code
        .map(_.filterNot(_.getName.contains("$")))
        .map(_.filterNot(_.getCanonicalPath.contains("/internal/")))
        // ignore project `hive` which depends on this project
        .map(_.filterNot(_.getCanonicalPath.contains("/hive/")))
    },
    // Ensure unidoc is run with tests. Must be cleaned before test for unidoc to be generated.
    (test in Test) := ((test in Test) dependsOn unidoc.in(Compile)).value
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
  (test in Test) := ((test in Test) dependsOn mimaReportBinaryIssues).value,
  mimaPreviousArtifacts := Set("io.delta" %% "delta-standalone" %  getPrevVersion(version.value)),
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
      "org.scalatest" %% "scalatest" % "3.1.0" % "test",
      "org.apache.spark" % "spark-sql_2.12" % "3.1.1" % "test",
      "io.delta" % "delta-core_2.12" % "1.0.0" % "test",
      "commons-io" % "commons-io" % "2.8.0" % "test",
      "org.apache.spark" % "spark-catalyst_2.12" % "3.1.1" % "test" classifier "tests",
      "org.apache.spark" % "spark-core_2.12" % "3.1.1" % "test" classifier "tests",
      "org.apache.spark" % "spark-sql_2.12" % "3.1.1" % "test" classifier "tests"
    )
  )

lazy val goldenTables = (project in file("golden-tables")) settings (
  name := "golden-tables",
  commonSettings,
  skipReleaseSettings,
  libraryDependencies ++= Seq(
    // Test Dependencies
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "org.apache.spark" % "spark-sql_2.12" % "3.0.0" % "test",
    "io.delta" % "delta-core_2.12" % "0.8.0" % "test",
    "commons-io" % "commons-io" % "2.8.0" % "test",
    "org.apache.spark" % "spark-catalyst_2.12" % "3.0.0" % "test" classifier "tests",
    "org.apache.spark" % "spark-core_2.12" % "3.0.0" % "test" classifier "tests",
    "org.apache.spark" % "spark-sql_2.12" % "3.0.0" % "test" classifier "tests"
  )
)

lazy val sqlDeltaImport = (project in file("sql-delta-import"))
  .settings (
    name := "sql-delta-import",
    commonSettings,
    publishArtifact := scalaBinaryVersion.value == "2.12",
    publishArtifact in Test := false,
    libraryDependencies ++= Seq(
      // We config this project to skip the publish step when running
      // `build/sbt "++ 2.11.12 publishLocal"` because it doesn't support Scala 2.11. However,
      // SBT will still try to resolve dependencies when running
      // `build/sbt "++ 2.11.12 publishLocal"` because Spark 3.0.0 and Delta 0.7.0 don't support
      // 2.11 and don't have the jars for Scala 2.11. In order to make our publish command pass, we
      // define Spark and Delta versions with 2.12 to make dependency resolution pass.
      "org.apache.spark" % "spark-sql_2.12" % "3.0.0" % "provided",
      "io.delta" % "delta-core_2.12" % "0.7.0" % "provided",
      "org.rogach" %% "scallop" % "3.5.1",
      "org.scalatest" %% "scalatest" % "3.1.1" % "test",
      "com.h2database" % "h2" % "1.4.200" % "test",
      "org.apache.spark" % "spark-catalyst_2.12" % "3.0.0" % "test",
      "org.apache.spark" % "spark-core_2.12" % "3.0.0" % "test",
      "org.apache.spark" % "spark-sql_2.12" % "3.0.0" % "test"
    )
  )
  .settings(releaseSettings)

val flinkVersion = "1.12.0"
lazy val flinkConnector = (project in file("flink-connector"))
  .settings (
    name := "flink-connector",
    commonSettings,
    publishArtifact := scalaBinaryVersion.value == "2.12",
    publishArtifact in Test := false,
    crossPaths := false,
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-core" % flinkVersion,
      "org.apache.flink" % "flink-connector-files" % flinkVersion,
      "org.apache.flink" % "flink-table-common" % flinkVersion,
      "org.apache.flink" %% "flink-parquet" % flinkVersion,
      "org.apache.flink" %% "flink-runtime" % flinkVersion,
      "org.apache.flink" %% "flink-table-runtime-blink" % flinkVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.flink" % "flink-connector-files" % flinkVersion % "test" classifier "tests",
      "org.apache.flink" %% "flink-streaming-java" % flinkVersion % "test",
      "org.apache.flink" % "flink-connector-test-utils" % flinkVersion % "test",
      "com.github.sbt" % "junit-interface" % "0.12" % Test
    )
  )
  .settings(skipReleaseSettings)
  .dependsOn(standalone)
