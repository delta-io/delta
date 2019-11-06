/*
 * Copyright 2019 Databricks, Inc.
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

lazy val commonSettings = Seq(
  version := "0.4.0",
  organization := "io.delta",
  scalaVersion := "2.12.8",
  fork := true
)

lazy val core = (project in file("core"))
  .settings(
    name := "delta-connector-core",
    libraryDependencies ++= Seq(
      "io.delta" %% "delta-core" % "0.4.0" excludeAll (ExclusionRule("org.apache.hadoop")),
      "org.apache.spark" %% "spark-sql" % "2.4.2" excludeAll (ExclusionRule("org.apache.hadoop")),
      "org.apache.hadoop" % "hadoop-client" % "2.6.5" % "provided"
    ),
    packageBin in Compile := assembly.value,
    commonSettings,
    assemblySettings
  )

lazy val coreTest = (project in file("coreTest"))
  .settings(
    commonSettings,
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.5",
    unmanagedJars in Compile += (packageBin in(core, Compile, packageBin)).value,
    autoScalaLibrary := false,

    // Ensures that the connector core jar is compiled before compiling this project
    (compile in Compile) := ((compile in Compile) dependsOn (packageBin in (core, Compile, packageBin))).value
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

  assemblyShadeRules in assembly := Seq(
    /*
      All org.apache.* before shading:
      arrow, avro, commons, curator, ivy, jute, log4j, orc, oro, parquet, spark, xbean, zookeeper
    */

    // Packages to exclude from shading because they are not happy when shaded
    ShadeRule.rename("javax.**" -> "@0").inAll,
    ShadeRule.rename("com.sun.**" -> "@0").inAll,
    ShadeRule.rename("com.fasterxml.**" -> "@0").inAll, // Scala reflect trigger via catalyst fails when package changed
    ShadeRule.rename("org.apache.hadoop.**" -> "@0").inAll, // Do not change any references to hadoop classes as they will be provided
    ShadeRule.rename("org.apache.spark.**" -> "@0").inAll, // Scala package object does not resolve correctly when package changed
    ShadeRule.rename("org.apache.log4j.**" -> "@0").inAll, // Initialization via reflection fails when package changed
    ShadeRule.rename("org.apache.commons.**" -> "@0").inAll, // Initialization via reflection fails when package changed
    ShadeRule.rename("org.xerial.snappy.*Native*" -> "@0").inAll, // JNI class fails to resolve native code when package changed
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

  logLevel in assembly := Level.Debug
)

