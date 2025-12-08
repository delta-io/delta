/*
 * Copyright (2025) The Delta Lake Project Authors.
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

// scalastyle:off line.size.limit

import ReleaseTransformations._
import Common._

/*
 * This is the root build.sbt for Delta Lake.
 * All project definitions have been moved to project/*.scala files for modularity.
 *
 * See project/SparkProjects.scala for Spark-related projects
 * See project/KernelProjects.scala for Kernel-related projects
 * See project/IcebergProjects.scala for Iceberg integration
 * See project/ConnectorProjects.scala for Standalone and other connectors
 * See project/StorageProjects.scala for storage layer
 * See project/ConnectProjects.scala for Spark Connect integration
 * See project/SharingProjects.scala for Delta Sharing
 * See project/HudiProjects.scala for Hudi integration
 */

// ============================================================
// Root-level settings
// ============================================================

Global / default_scala_version := Common.scala213
scalaVersion := default_scala_version.value
crossScalaVersions := Nil
Global / targetJvm := "11"
ThisBuild / parallelExecution := false

// ============================================================
// Storage Projects
// ============================================================
lazy val storage = StorageProjects.storage
lazy val storageS3DynamoDB = StorageProjects.storageS3DynamoDB

// ============================================================
// Connector Projects (Standalone, Golden Tables, etc.)
// ============================================================
lazy val goldenTables = ConnectorProjects.goldenTables
lazy val standalone = ConnectorProjects.standalone
lazy val standaloneWithoutParquetUtils = ConnectorProjects.standaloneWithoutParquetUtils
lazy val standaloneParquet = ConnectorProjects.standaloneParquet
lazy val standaloneCosmetic = ConnectorProjects.standaloneCosmetic
lazy val testStandaloneCosmetic = ConnectorProjects.testStandaloneCosmetic
lazy val testParquetUtilsWithStandaloneCosmetic = ConnectorProjects.testParquetUtilsWithStandaloneCosmetic
lazy val sqlDeltaImport = ConnectorProjects.sqlDeltaImport

// ============================================================
// Kernel Projects
// ============================================================
lazy val kernelApi = KernelProjects.kernelApi
lazy val kernelDefaults = KernelProjects.kernelDefaults
lazy val kernelBenchmarks = KernelProjects.kernelBenchmarks
lazy val kernelUnityCatalog = KernelProjects.kernelUnityCatalog
lazy val kernelGroup = KernelProjects.kernelGroup

// ============================================================
// Spark Projects (V1, V2, Unified, Contribs)
// ============================================================
lazy val deltaSuiteGenerator = SparkProjects.deltaSuiteGenerator
lazy val sparkV1 = SparkProjects.sparkV1
lazy val sparkV1Filtered = SparkProjects.sparkV1Filtered
lazy val sparkV2 = SparkProjects.sparkV2
lazy val spark = SparkProjects.spark
lazy val contribs = SparkProjects.contribs
lazy val sparkGroup = SparkProjects.sparkGroup

// ============================================================
// Spark Connect Projects
// ============================================================
lazy val connectCommon = ConnectProjects.connectCommon
lazy val connectClient = ConnectProjects.connectClient
lazy val connectServer = ConnectProjects.connectServer

// ============================================================
// Iceberg Projects
// ============================================================
lazy val testDeltaIcebergJar = IcebergProjects.testDeltaIcebergJar
lazy val icebergShaded = IcebergProjects.icebergShaded
lazy val iceberg = IcebergProjects.iceberg
lazy val icebergGroup = IcebergProjects.icebergGroup

// ============================================================
// Delta Sharing
// ============================================================
lazy val sharing = SharingProjects.sharing

// ============================================================
// Hudi Integration
// ============================================================
lazy val hudi = HudiProjects.hudi

// ============================================================
// Root project settings
// ============================================================
publishArtifact := false  // Don't release the root project
publish / skip := true
publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
releaseCrossBuild := false  // Don't use sbt-release's cross facility
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease
) ++ CrossSparkVersions.crossSparkReleaseSteps("+publishSigned") ++ Seq[ReleaseStep](
  // Do NOT use `sonatypeBundleRelease` - it will actually release to Maven! We want to do that
  // manually.
  //
  // Do NOT use `sonatypePromote` - it will promote the closed staging repository (i.e. sync to
  //                                Maven central)
  //
  // See https://github.com/xerial/sbt-sonatype#publishing-your-artifact.
  //
  // - sonatypePrepare: Drop the existing staging repositories (if exist) and create a new staging
  //                    repository using sonatypeSessionName as a unique key
  // - sonatypeBundleUpload: Upload your local staging folder contents to a remote Sonatype
  //                         repository
  // - sonatypeClose: closes your staging repository at Sonatype. This step verifies Maven central
  //                  sync requirement, GPG-signature, javadoc and source code presence, pom.xml
  //                  settings, etc
  // TODO: this isn't working yet
  // releaseStepCommand("sonatypePrepare; sonatypeBundleUpload; sonatypeClose"),
  setNextVersion,
  commitNextVersion
)
