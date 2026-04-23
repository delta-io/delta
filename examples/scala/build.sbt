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

val scala213 = "2.13.17"
val icebergVersion = "1.4.1"
val unityCatalogVersion = "0.4.1"

def resolveJacksonVersionForSpark(sparkVersion: String): String = sparkVersion match {
  case v if v.startsWith("4.2") => "2.21.2"
  case v if v.startsWith("4.1") => "2.20.0"
  case v if v.startsWith("4.0") => "2.18.2"
  case _ => "2.15.4"
}

def resolveJacksonAnnotationsVersionForSpark(sparkVersion: String): String = sparkVersion match {
  case v if v.startsWith("4.2") => "2.21"
  case v if v.startsWith("4.1") => "2.20"
  case v if v.startsWith("4.0") => "2.18.2"
  case _ => resolveJacksonVersionForSpark(sparkVersion)
}

val defaultDeltaVersion = {
  val versionFileContent = IO.read(file("../../version.sbt"))
  val versionRegex = """.*version\s*:=\s*"([^"]+)".*""".r
  versionRegex.findFirstMatchIn(versionFileContent) match {
    case Some(m) => m.group(1)
    case None => throw new Exception("Could not parse version from version.sbt")
  }
}

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
// Maps Delta version (major, minor) to the compatible Spark version.
// Used as a fallback for local dev when SPARK_VERSION env var is not set.
val lookupSparkVersion: PartialFunction[(Int, Int), String] = {
  // TODO: how to run integration tests for multiple Spark versions
  case (major, minor) if major >= 4 && minor >= 1 => "4.1.0"
  // version 4.0.0
  case (major, minor) if major >= 4 => "4.0.0"
  // versions 3.3.x+
  case (major, minor) if major >= 3 && minor >=3 => "3.5.3"
  // versions 3.0.0 to 3.2.x
  case (major, minor) if major >= 3 && minor <=2 => "3.5.0"
  // versions 2.4.x
  case (major, minor) if major == 2 && minor == 4 => "3.4.0"
  // versions 2.3.x
  case (major, minor) if major == 2  && minor == 3 => "3.3.2"
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
  s"get delta version from environment variable DELTA_VERSION. If it doesn't exist, use $defaultDeltaVersion"
)
val getDeltaArtifactName = settingKey[String](
  s"get delta artifact name based on the delta version. either `delta-core` or `delta-spark`."
)
val getJacksonVersion = settingKey[String](
  "get the Jackson version compatible with the resolved Spark version."
)
val getJacksonAnnotationsVersion = settingKey[String](
  "get the jackson-annotations version compatible with the resolved Spark version."
)
val getIcebergSparkRuntimeArtifactName = settingKey[String](
  s"get iceberg-spark-runtime name based on the delta version."
)
getScalaVersion := {
  sys.env.get("SCALA_VERSION") match {
    case Some("2.13") | Some(`scala213`) =>
      scala213
    case Some(v) =>
      println(
        s"[warn] Invalid  SCALA_VERSION. Expected one of {2.13, $scala213} but " +
        s"got $v. Fallback to $scala213."
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
      println(s"Using DELTA_VERSION Delta version $v")
      v
    case None =>
      println(s"Using default Delta version $defaultDeltaVersion")
      defaultDeltaVersion
  }
}

getDeltaArtifactName := {
  val deltaVersion = getDeltaVersion.value
  if (deltaVersion.charAt(0).asDigit >= 3) "delta-spark" else "delta-core"
}

getJacksonVersion := {
  resolveJacksonVersionForSpark(resolveSparkVersion(getDeltaVersion.value))
}

getJacksonAnnotationsVersion := {
  resolveJacksonAnnotationsVersionForSpark(resolveSparkVersion(getDeltaVersion.value))
}

val getSparkPackageSuffix = settingKey[String](
  s"get package suffix for cross-build artifact name from environment variable SPARK_PACKAGE_SUFFIX. " +
  s"This is derived from CrossSparkVersions.scala (single source of truth)."
)

getSparkPackageSuffix := {
  sys.env.getOrElse("SPARK_PACKAGE_SUFFIX", "")
}

val getSupportIceberg = settingKey[String](
  s"get supportIceberg for cross-build artifact name from environment variable SUPPORT_ICEBERG. " +
  s"This is derived from CrossSparkVersions.scala (single source of truth)."
)

getSupportIceberg := {
  sys.env.getOrElse("SUPPORT_ICEBERG", "false")
}

getIcebergSparkRuntimeArtifactName := {
  val (expMaj, expMin) = getMajorMinor(lookupSparkVersion.apply(
    getMajorMinor(getDeltaVersion.value)))
  s"iceberg-spark-runtime-$expMaj.$expMin"
}

lazy val extraMavenRepo = sys.env.get("EXTRA_MAVEN_REPO").toSeq.map { repo =>
  resolvers += "Delta" at repo
}

lazy val java17Settings = Seq(
  fork := true,
  javaOptions ++= Seq(
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
  )
)

// Use SPARK_VERSION env var if set, otherwise fall back to lookupSparkVersion (for local dev)
def resolveSparkVersion(deltaVersion: String): String = {
  val envVersion = sys.env.getOrElse("SPARK_VERSION", "")
  if (envVersion.nonEmpty) envVersion
  else lookupSparkVersion.apply(getMajorMinor(deltaVersion))
}

def getLibraryDependencies(
    deltaVersion: String,
    deltaArtifactName: String,
    icebergSparkRuntimeArtifactName: String,
    sparkPackageSuffix: String,
    scalaBinVersion: String,
    supportIceberg: String): Seq[ModuleID] = {

  // Package suffix comes from CrossSparkVersions.scala (single source of truth)
  // e.g., "" for default Spark, "_4.1" for Spark 4.1
  val deltaCoreDep = "io.delta" % s"${deltaArtifactName}${sparkPackageSuffix}_${scalaBinVersion}" % deltaVersion
  val deltaIcebergDep = "io.delta" % s"delta-iceberg_${scalaBinVersion}" % deltaVersion

  val resolvedSparkVersion = resolveSparkVersion(deltaVersion)

  val baseDeps = Seq(
    deltaCoreDep,
    "org.apache.spark" %% "spark-sql" % resolvedSparkVersion,
    "org.apache.spark" %% "spark-hive" % resolvedSparkVersion,
    "org.apache.iceberg" % "iceberg-hive-metastore" % icebergVersion
  )

  // Include Iceberg dependencies only if supportIceberg is enabled
  val icebergDeps = if (supportIceberg == "true") {
    getMajorMinor(deltaVersion) match {
      case (major, _) if major >= 4 =>
        // Don't include the iceberg dependencies for 4.0.0rc1 and later
        Seq.empty
      case _ =>
        Seq(
          deltaIcebergDep,
          "org.apache.iceberg" %% icebergSparkRuntimeArtifactName % icebergVersion,
        )
    }
  } else {
    Seq.empty
  }

  baseDeps ++ icebergDeps
}

lazy val root = (project in file("."))
  .settings(
    run / fork := true,
    name := "hello-world",
    crossScalaVersions := Seq(scala213),
    libraryDependencies ++= getLibraryDependencies(
      getDeltaVersion.value,
      getDeltaArtifactName.value,
      getIcebergSparkRuntimeArtifactName.value,
      getSparkPackageSuffix.value,
      scalaBinaryVersion.value,
      getSupportIceberg.value),
    libraryDependencies ++= Seq(
      "io.unitycatalog" %% "unitycatalog-spark" % unityCatalogVersion excludeAll(
        ExclusionRule(organization = "com.fasterxml.jackson.core"),
        ExclusionRule(organization = "com.fasterxml.jackson.module"),
        ExclusionRule(organization = "com.fasterxml.jackson.datatype"),
        ExclusionRule(organization = "com.fasterxml.jackson.dataformat")
      ),
      "io.unitycatalog" % "unitycatalog-server" % unityCatalogVersion excludeAll(
        ExclusionRule(organization = "com.fasterxml.jackson.core"),
        ExclusionRule(organization = "com.fasterxml.jackson.module"),
        ExclusionRule(organization = "com.fasterxml.jackson.datatype"),
        ExclusionRule(organization = "com.fasterxml.jackson.dataformat")
      )
    ),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % getJacksonVersion.value,
      "com.fasterxml.jackson.core" % "jackson-annotations" %
        getJacksonAnnotationsVersion.value,
      "com.fasterxml.jackson.core" % "jackson-databind" % getJacksonVersion.value,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % getJacksonVersion.value,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % getJacksonVersion.value,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % getJacksonVersion.value,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % getJacksonVersion.value
    ),
    extraMavenRepo,
    resolvers += Resolver.mavenLocal,
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature"
    ),
    // Conditionally exclude IcebergCompatV2.scala when supportIceberg is "false"
    Compile / unmanagedSources / excludeFilter := {
      if (getSupportIceberg.value == "false") {
        HiddenFileFilter || "IcebergCompatV2.scala"
      } else {
        HiddenFileFilter
      }
    },
    java17Settings
  )
