/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import ReleaseTransformations._

parallelExecution in ThisBuild := false
scalastyleConfig in ThisBuild := baseDirectory.value / "scalastyle-config.xml"
crossScalaVersions in ThisBuild := Seq("2.12.8", "2.11.12")

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

val sparkVersion = "2.4.3"
val hadoopVersion = "3.1.0"
val hiveVersion = "3.1.2"
val tezVersion = "0.9.2"
val hiveDeltaVersion = "0.5.0"

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := "2.12.8",
  fork := true,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions += "-target:jvm-1.8",
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
  (test in Test) := ((test in Test) dependsOn testScalastyle).value
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

lazy val hive = (project in file("hive")) dependsOn(standalone) settings (
  name := "delta-hive",
  commonSettings,
  releaseSettings,

  // Minimal dependencies to compile the codes. This project doesn't run any tests so we don't need
  // any runtime dependencies.
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.parquet" % "parquet-hadoop" % "1.10.1" % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" classifier "core" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.apache.hive" % "hive-metastore" % hiveVersion % "provided"  excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.apache.hive", "hive-exec")
    ),
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule("org.apache.hive", "hive-exec"),
      ExclusionRule("com.google.guava", "guava"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  ),

  /** Hive assembly jar. Build with `assembly` command */
  logLevel in assembly := Level.Info,
  test in assembly := {},
  assemblyJarName in assembly := s"${name.value}-assembly_${scalaBinaryVersion.value}-${version.value}.jar",
  // default merge strategy
  assemblyShadeRules in assembly := Seq(
    /**
     * Hive 2.3.7 uses an old paranamer version that doesn't support Scala 2.12
     * (https://issues.apache.org/jira/browse/SPARK-22128), so we need to shade our own paranamer
     * version to avoid conflicts.
     */
    ShadeRule.rename("com.thoughtworks.paranamer.**" -> "shadedelta.@0").inAll
    )
)

lazy val hiveMR = (project in file("hive-mr")) dependsOn(hive % "test->test") settings (
  name := "hive-mr",
  commonSettings,
  skipReleaseSettings,
  unmanagedResourceDirectories in Test += file("golden-tables/src/test/resources"),
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("com.google.guava", "guava"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )
)

lazy val hiveTez = (project in file("hive-tez")) dependsOn(hive % "test->test") settings (
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
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.jodd" % "jodd-core" % "3.5.2",
    "org.apache.hive" % "hive-metastore" % hiveVersion % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule(organization = "org.eclipse.jetty"),
      ExclusionRule("org.apache.hive", "hive-exec")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-hs" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "test" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion % "test" classifier "tests",
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
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

lazy val standalone = (project in file("standalone"))
  .settings(
    name := "delta-standalone",
    commonSettings,
    releaseSettings,
    mimaSettings,
    unmanagedResourceDirectories in Test += file("golden-tables/src/test/resources"),
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.apache.parquet" % "parquet-hadoop" % "1.10.1" % "provided",
      "com.github.mjakubowski84" %% "parquet4s-core" % "1.2.1" excludeAll (
        ExclusionRule("org.slf4j", "slf4j-api"),
        ExclusionRule("org.apache.parquet", "parquet-hadoop")
      ),
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8",
      "org.json4s" %% "json4s-jackson" % "3.5.3" excludeAll (
        ExclusionRule("com.fasterxml.jackson.core"),
        ExclusionRule("com.fasterxml.jackson.module")
      ),
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    ))

  /**
   * Unidoc settings
   * Generate javadoc with `unidoc` command, outputs to `standalone/target/javaunidoc`
   */
  .enablePlugins(GenJavadocPlugin, JavaUnidocPlugin)
  .settings(
    javacOptions in (JavaUnidoc, unidoc) := Seq(
      "-public",
      "-windowtitle", "Delta Standalone Reader " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
      "-noqualifier", "java.lang",
      "-tag", "return:X",
      // `doclint` is disabled on Circle CI. Need to enable it manually to test our javadoc.
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
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
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
