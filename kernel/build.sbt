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

  // Can be run explicitly via: build/sbt $module/checkstyle
  // Will automatically be run during compilation (e.g. build/sbt compile)
  // and during tests (e.g. build/sbt test)
  checkstyleConfigLocation := CheckstyleConfigLocation.File("dev/checkstyle.xml"),
  checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error),
  (checkstyle in Compile) := (checkstyle in Compile).triggeredBy(compile in Compile).value,
  (checkstyle in Test) := (checkstyle in Test).triggeredBy(compile in Test).value
)

// TODO: after adding scala source files SBT will no longer automatically run javadoc instead of
//  scaladoc

lazy val kernelApi = (project in file("kernel-api"))
  .settings(
    name := "delta-kernel-api",
    commonSettings,
    scalaStyleSettings,
    releaseSettings,
    libraryDependencies ++= Seq(
      "org.roaringbitmap" % "RoaringBitmap" % "0.9.25",

      "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5" % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "junit" % "junit" % "4.11" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test"
    ),
    Compile / doc / javacOptions := Seq(
      "-public",
      "-windowtitle", "Delta Kernel API " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
      "-noqualifier", "java.lang",
      "-Xdoclint:all"
      // TODO: exclude internal packages
    ),
    Compile / doc / sources := {
      (Compile / doc / sources).value
        // exclude internal classes
        .filterNot(_.getCanonicalPath.contains("/internal/"))
    },
    // Ensure doc is run with tests. Must be cleaned before test for docs to be generated
    (Test / test) := ((Test / test) dependsOn (Compile / doc)).value
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
      "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion, // Configuration, Path
      "io.delta" % "delta-storage" % deltaStorageVersion, // LogStore
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5", // ObjectMapper
      "org.apache.parquet" % "parquet-hadoop" % "1.12.3",

      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "io.delta" %% "delta-core" % deltaSparkVersion % "test",
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
