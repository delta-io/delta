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
import scala.util.{Success, Try}
name := "example"
organization := "com.example"
organizationName := "example"

val scala212 = "2.12.15"
val scala213 = "2.13.8"
val deltaVersion = "2.1.0"

def getMajorMinor(version: String): (Int, Int) = {
  val majorMinor = Try {
    val splitVersion = version.split('.')
    (splitVersion(0).toInt, splitVersion(1).toInt)
  }
  majorMinor match {
    case Success(_) => (majorMinor.get._1, majorMinor.get._2)
    case _ =>
      throw new RuntimeException(s"Unsupported delta version: $version. " +
        s"Please check https://docs.delta.io/latest/releases.html")
  }
}
val lookupSparkVersion: PartialFunction[(Int, Int), String] = {
  // versions 2.3.0 and above
  case (major, minor) if (major == 2  && minor >= 3) || major >= 3 => "3.3.2"
  // versions 2.2.x
  case (major, minor) if major == 2  && minor == 2 => "3.3.1"
  // versions 2.1.x
  case (major, minor) if major == 2  && minor == 1 => "3.3.0"
  // versions 1.0.0 to 2.0.x
  case (major, minor) if major == 1 || (major == 2 && minor == 0) => "3.2.1"
  // versions 0.7.x to 0.8.x
  case (major, minor) if major == 0 && (minor == 7 || minor == 8) => "3.0.2"
  // versions below 0.7
  case (major, minor) if major == 0 && minor < 7 => "2.4.4"
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
      deltaVersion
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
        getMajorMinor(getDeltaVersion.value)
      )
    ),
    extraMavenRepo,
    resolvers += Resolver.mavenLocal,
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature"
    )
  )
