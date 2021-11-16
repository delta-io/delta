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

name := "example"
organization := "com.example"
organizationName := "example"

val scala212 = "2.12.14"
val scala213 = "2.13.5"

scalaVersion := scala212
version := "0.1.0"

def getDeltaVersion(): String = {
  val envVars = System.getenv
  if (envVars.containsKey("DELTA_VERSION")) {
    val version = envVars.get("DELTA_VERSION")
    println("Using Delta version " + version)
    version
  } else {
    "1.1.0"
  }
}

lazy val extraMavenRepo = sys.env.get("EXTRA_MAVEN_REPO").toSeq.map { repo => 
  resolvers += "Delta" at repo
}

lazy val root = (project in file("."))
  .settings(
    name := "hello-world",
    crossScalaVersions := Seq(scala212, scala213),
    libraryDependencies += "io.delta" %% "delta-core" % getDeltaVersion(),
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0",
    extraMavenRepo
  )
  
