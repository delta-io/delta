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

val scala212 = "2.12.15"
scalaVersion := scala212

lazy val commonSettings = Seq(
  organization := "io.delta",
  scalaVersion := scala212,
  fork := true,
  scalacOptions ++= Seq("-target:jvm-1.8", "-Ywarn-unused:imports"),
  javacOptions ++= Seq("-source", "1.8"),
  // -target cannot be passed as a parameter to javadoc. See https://github.com/sbt/sbt/issues/355
  Compile / compile / javacOptions ++= Seq("-target", "1.8", "-Xlint:unchecked"),
  // Configurations to speed up tests and reduce memory footprint
  Test / javaOptions += "-Xmx1024m",
)

// TODO javastyle checkstyle tests
// TODO unidoc/javadoc settings

lazy val kernelApi = (project in file("kernel-api"))
  .settings(
    name := "delta-kernel-api",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    libraryDependencies ++= Seq()
  )

val hadoopVersion = "3.3.1"
val deltaStorageVersion = "2.2.0"
val scalaTestVersion = "3.2.15"
val deltaSparkVersion = deltaStorageVersion
val sparkVersion = "3.3.2"

lazy val kernelDefault = (project in file("kernel-default"))
  .dependsOn(kernelApi)
  .settings(
    name := "delta-kernel-default",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion, // Configuration, Path
      "io.delta" % "delta-storage" % deltaStorageVersion, // LogStore
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5", // ObjectMapper
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3",

      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "io.delta" %% "delta-core" % deltaSparkVersion % "test",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test", // SparkSession
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
      "junit" % "junit" % "4.11" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test"
    )
  )

/*
 ***********************
 * ScalaStyle settings *
 ***********************
 */
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

// Not used since scala is test-only
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val scalaStyleSettings = Seq(
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  Compile / compile := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  Test / test := ((Test / test) dependsOn testScalastyle).value
)

/*
 ********************
 * Release settings *
 ********************
 */

// Don't release the root project
publishArtifact := false
publish / skip := true

lazy val releaseSettings = Seq(
  // Java only release settings
  crossPaths := false, // drop off Scala suffix from artifact names
  autoScalaLibrary := false, // exclude scala-library from dependencies in generated pom.xml

  // Other release settings
  publishArtifact := true,
  Test / publishArtifact := false
)
