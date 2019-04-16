/* Copyright (C) 2017 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

name := "delta"

organization := "com.databricks"

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.12.8", "2.11.12")

libraryDependencies ++= Seq(
  // Adding test classifier seems to break transitive resolution of the core dependencies
  "org.apache.spark" %% "spark-hive" % "2.4.1",
  "org.apache.spark" %% "spark-sql" % "2.4.1",
  "org.apache.spark" %% "spark-core" % "2.4.1",
  "org.apache.spark" %% "spark-catalyst" % "2.4.1",

  // Test deps
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-catalyst" % "2.4.1" % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % "2.4.1" % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % "2.4.1" % "test" classifier "tests"
)

testOptions in Test += Tests.Argument("-oF")

// Don't execute in parallel since we can't have multiple Sparks in the same JVM
parallelExecution in Test := false

scalacOptions ++= Seq("-target:jvm-1.8")

javaOptions += "-Xmx3g"

fork in Test := true

// Configurations to speed up tests and reduce memory footprint
javaOptions in Test ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Dspark.databricks.delta.snapshotPartitions=2",
  "-Dspark.sql.shuffle.partitions=5"
)
