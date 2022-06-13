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
import scala.util.Try
name := "example"
organization := "com.example"
organizationName := "example"

val scala212 = "2.12.15"
val scala213 = "2.13.8"
val deltaVersion = "1.2.0"

val lookupSparkVersion: PartialFunction[String, String] = {
  case v10x_and_above if Try {
        v10x_and_above.split('.')(0).toInt
      }.toOption.exists(_ >= 1) =>
    "3.2.1"
  case v07x_v08x
      if v07x_v08x.startsWith("0.7") || v07x_v08x.startsWith("0.8") =>
    "3.0.2"
  case belowv07 if Try {
        belowv07.split('.')(1).toInt
      }.toOption.exists(_ < 7) =>
    "2.4.4"
  case v =>
    throw new RuntimeException(
      s"Unsupported delta version: $v. Please check https://docs.delta.io/latest/releases.html"
    )
}

val getScalaVersion = settingKey[String](
  s"get scala version from environment variable SCALA_VERSION. If it doesn't exist, use $scala213"
)
val getDeltaVersion = settingKey[String](
  s"get delta version from environment variable DELTA_VERSION. If it doesn't exist, use $deltaVersion"
)

getScalaVersion := {
  sys.env.get("SCALA_VERSION") match {
    case Some("2.12") =>
      scala212
    case Some("2.13") =>
      scala213
    case Some(v) =>
      println(
        s"[warn] Invalid  SCALA_VERSION. Expected 2.12 or 2.13 but got $v. Fallback to $scala213."
      )
      scala213
    case None =>
      scala213
  }
}

scalaVersion := getScalaVersion.value
version := "0.1.0"

getDeltaVersion := {
  sys.env.get("DELTA_VERSION") match {
    case Some(v) =>
      println(s"Using Delta version $v")
      v
    case None =>
      "1.2.0"
  }
}

lazy val extraMavenRepo = sys.env.get("EXTRA_MAVEN_REPO").toSeq.map { repo =>
  resolvers += "Delta" at repo
}

lazy val root = (project in file("."))
  .settings(
    run / fork := true,
    name := "hello-world",
    crossScalaVersions := Seq(scala212, scala213),
    libraryDependencies ++= Seq(
      "io.delta" %% "delta-core" % getDeltaVersion.value,
      "org.apache.spark" %% "spark-sql" % lookupSparkVersion.apply(
        getDeltaVersion.value
      )
    ),
    extraMavenRepo,
    resolvers += Resolver.mavenLocal,
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature"
    )
  )
