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

parallelExecution in ThisBuild := false
scalastyleConfig in ThisBuild := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

crossScalaVersions := Seq("2.12.8", "2.11.12")

val sparkVersion = "2.4.3"
val hadoopVersion = "2.7.2"
val hiveVersion = "2.3.3"
val deltaVersion = "0.5.0"

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := "2.12.8",
  fork := true,
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

lazy val core = (project in file("core"))
  .settings(
    name := "delta-core-shaded",
    libraryDependencies ++= Seq(
      "io.delta" %% "delta-core" % deltaVersion excludeAll ExclusionRule("org.apache.hadoop"),
      "org.apache.spark" %% "spark-sql" % sparkVersion excludeAll(
        ExclusionRule("org.apache.hadoop"),
        // Remove all dependencies used by Spark UI. Spark UI is not needed and we have disabled it.
        // So these dependencies are not used any more.
        ExclusionRule("com.sun.jersey"),
        ExclusionRule("org.glassfish"),
        ExclusionRule("org.glassfish.jersey.bundles"),
        ExclusionRule("org.glassfish.jersey.media"),
        ExclusionRule("org.glassfish.hk2"),
        ExclusionRule("org.glassfish.jersey.bundles.repackaged"),
        ExclusionRule("org.glassfish.jersey.test-framework"),
        ExclusionRule("org.glassfish.hk2.external"),
        ExclusionRule("org.glassfish.jersey.containers"),
        ExclusionRule("org.glassfish.jersey.test-framework.providers"),
        ExclusionRule("org.glassfish.jaxb"),
        ExclusionRule("org.glassfish.jersey.core"),
        ExclusionRule("org.glassfish.jersey"),
        ExclusionRule("org.glassfish.jersey.inject"),
        ExclusionRule("javax.ws.rs"),
        ExclusionRule("org.xerial.snappy")
    ),
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"
    ),

    // Make the 'compile' invoke the 'assembly' task to generate the uber jar.
    packageBin in Compile := assembly.value,

    commonSettings,
    assemblySettings
  )

lazy val coreTest = (project in file("coreTest"))
  .settings(
    // Add the uber jar as unmanaged library so that we don't bring in the transitive dependencies
    unmanagedJars in Compile += (packageBin in(core, Compile, packageBin)).value,

    // Only dependency not in the uber jar
    libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll
      ExclusionRule("org.slf4j", "slf4j-log4j12"),
      "org.xerial.snappy" % "snappy-java" % "1.1.7.3"
    ),

    autoScalaLibrary := false,

    // Ensure that the uber jar is compiled before compiling this project
    (compile in Compile) := ((compile in Compile) dependsOn (packageBin in (core, Compile, packageBin))).value,

    // Make 'test' invoke 'runMain'
    test in Test := (runMain in Runtime).toTask(" test.Test").value,

    commonSettings
  )

lazy val assemblySettings = Seq(
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case "log4j.properties" => MergeStrategy.discard
    case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  },
  assemblyJarName in assembly := s"${name.value}-assembly_${scalaBinaryVersion.value}-${version.value}.jar",

  assemblyShadeRules in assembly :=
    (if (scalaBinaryVersion.value == "2.11") Seq(
      // json4s cannot be shaded when using Scala 2.11
      ShadeRule.rename("org.json4s.**" -> "@0").inAll
    ) else Nil) ++ Seq(
    /*
      All org.apache.* before shading:
      arrow, avro, commons, curator, ivy, jute, log4j, orc, oro, parquet, spark, xbean, zookeeper
    */

    ShadeRule.rename("org.apache.commons.lang3.time.**" -> "shadedelta.@0").inAll,

    // Packages to exclude from shading because they are not happy when shaded
    ShadeRule.rename("javax.**" -> "@0").inAll,
    ShadeRule.rename("com.sun.**" -> "@0").inAll,
    ShadeRule.rename("com.fasterxml.**" -> "@0").inAll, // Scala reflect trigger via catalyst fails when package changed
    ShadeRule.rename("org.apache.hadoop.**" -> "@0").inAll, // Do not change any references to hadoop classes as they will be provided
    ShadeRule.rename("org.apache.spark.**" -> "@0").inAll, // Scala package object does not resolve correctly when package changed
    ShadeRule.rename("org.apache.log4j.**" -> "@0").inAll, // Initialization via reflection fails when package changed
    ShadeRule.rename("org.slf4j.**" -> "@0").inAll, // Initialization via reflection fails when package changed
    ShadeRule.rename("org.apache.commons.**" -> "@0").inAll, // Initialization via reflection fails when package changed
    ShadeRule.rename("org.xerial.snappy.**" -> "@0").inAll, // Snappy fails to resolve native code when package changed
    ShadeRule.rename("com.databricks.**" -> "@0").inAll, // Scala package object does not resolve correctly when package changed

    // Shade everything else
    ShadeRule.rename("com.**" -> "shadedelta.@0").inAll,
    ShadeRule.rename("org.**" -> "shadedelta.@0").inAll,
    ShadeRule.rename("io.**" -> "shadedelta.@0").inAll,
    ShadeRule.rename("net.**" -> "shadedelta.@0").inAll,
    ShadeRule.rename("avro.**" -> "shadedelta.@0").inAll,
    ShadeRule.rename("codegen.**" -> "shadedelta.@0").inAll,
    ShadeRule.rename("jersey.**" -> "shadedelta.@0").inAll,
    ShadeRule.rename("javassist.**" -> "shadedelta.@0").inAll,

    /*
      All top level dirs left in the jar after shading:
      aix, assets, com, darwin, delta, fr, include, javax, linux, org, scala, shaded, shadedelta, win 
    */

    // Remove things we know are not needed
    ShadeRule.zap("py4j**").inAll,
    ShadeRule.zap("webapps**").inAll,
    ShadeRule.zap("delta**").inAll
  ),

  logLevel in assembly := Level.Info
)

lazy val hive = (project in file("hive")) settings (
  name := "hive-delta",
  commonSettings,
  unmanagedJars in Compile += (packageBin in(core, Compile, packageBin)).value,
  autoScalaLibrary := false,

  // Ensures that the connector core jar is compiled before compiling this project
  (compile in Compile) := ((compile in Compile) dependsOn (packageBin in (core, Compile, packageBin))).value,

  // Minimal dependencies to compile the codes. This project doesn't run any tests so we don't need
  // any runtime dependencies.
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.apache.hive" % "hive-cli" % hiveVersion % "test" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("ch.qos.logback", "logback-classic"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "io.delta" %% "delta-core" % deltaVersion % "test"
  )
)

lazy val hiveMR = (project in file("hive-mr")) dependsOn(hive % "test->test") settings (
  name := "hive-mr",
  commonSettings,
  unmanagedJars in Compile += (packageBin in(core, Compile, packageBin)).value,
  autoScalaLibrary := false,

  // Ensures that the connector core jar is compiled before compiling this project
  (compile in Compile) := ((compile in Compile) dependsOn (packageBin in (core, Compile, packageBin))).value,

  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
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
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm")
    ),
    // TODO Figure out how this fixes some bad dependency
    "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "io.delta" %% "delta-core" % deltaVersion % "test" excludeAll ExclusionRule("org.apache.hadoop")
  )
)

lazy val hiveTez = (project in file("hive-tez")) dependsOn(hive % "test->test") settings (
  name := "hive-tez",
  commonSettings,
  unmanagedJars in Compile += (packageBin in(core, Compile, packageBin)).value,
  autoScalaLibrary := false,
  // Ensures that the connector core jar is compiled before compiling this project
  (compile in Compile) := ((compile in Compile) dependsOn (packageBin in (core, Compile, packageBin))).value,

  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"  excludeAll (
      ExclusionRule(organization = "com.google.protobuf")
      ),
    "com.google.protobuf" % "protobuf-java" % "2.5.0",
    "org.apache.hive" % "hive-exec" % hiveVersion % "provided" excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.apache.parquet"),
      ExclusionRule("org.pentaho", "pentaho-aggdesigner-algorithm"),
      ExclusionRule(organization = "com.google.protobuf")
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
      ExclusionRule(organization = "com.google.protobuf")
    ),
    "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % "test",
    "org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion % "test",
    "org.apache.tez" % "tez-mapreduce" % "0.8.4" % "test",
    "org.apache.tez" % "tez-dag" % "0.8.4" % "test",
    "org.apache.tez" % "tez-tests" % "0.8.4" % "test" classifier "tests",
    // TODO Figure out how this fixes some bad dependency
    "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "io.delta" %% "delta-core" % deltaVersion % "test" excludeAll ExclusionRule("org.apache.hadoop")
  )
)
