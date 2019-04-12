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
  "org.apache.spark" %% "spark-hive" % "2.4.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "test" classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % "2.4.0" % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % "2.4.0" % "test" classifier "tests"
)

// Display full-length stacktraces from ScalaTest:
testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-target:jvm-1.7")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")