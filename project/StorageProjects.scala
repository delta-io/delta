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

import sbt._
import Keys._
import com.lightbend.sbt.JavaFormatterPlugin
import org.scalafmt.sbt.ScalafmtPlugin
import Common._
import Unidoc._

/**
 * Storage layer projects - LogStore implementations and related abstractions.
 */
object StorageProjects {

  /**
   * Delta Storage - Core storage abstraction layer (LogStore interface).
   */
  lazy val storage = (project in file("storage"))
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-storage",
      commonSettings,
      exportJars := true,
      javaOnlyReleaseSettings,
      libraryDependencies ++= Seq(
        // User can provide any 2.x or 3.x version. We don't use any new fancy APIs. Watch out for
        // versions with known vulnerabilities.
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",

        // Note that the org.apache.hadoop.fs.s3a.Listing::createFileStatusListingIterator 3.3.1 API
        // is not compatible with 3.3.2.
        "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "provided",

        // Test Deps
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      ),

      // Unidoc settings
      unidocSourceFilePatterns += SourceFilePattern("/LogStore.java", "/CloseableIterator.java"),
    ).configureUnidoc()

  /**
   * S3 + DynamoDB LogStore implementation.
   */
  lazy val storageS3DynamoDB = (project in file("storage-s3-dynamodb"))
    .dependsOn(storage % "compile->compile;test->test;provided->provided")
    .dependsOn(SparkProjects.spark % "test->test")
    .disablePlugins(JavaFormatterPlugin, ScalafmtPlugin)
    .settings(
      name := "delta-storage-s3-dynamodb",
      commonSettings,
      javaOnlyReleaseSettings,

      // uncomment only when testing FailingS3DynamoDBLogStore. this will include test sources in
      // a separate test jar.
      // Test / publishArtifact := true,

      libraryDependencies ++= Seq(
        "com.amazonaws" % "aws-java-sdk" % "1.12.262" % "provided",

        // Test Deps
        "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "test", // RemoteFileChangedException
      )
    ).configureUnidoc()
}
