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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.{DeltaLog, DeltaManifest, DeltaManifestWriter, ManifestWriterProvider, Snapshot}
import org.apache.spark.sql.SparkSession

/**
 * Configuration for the manifest.
 */
case class GenerateManifestOptions (
   path: Option[String],
   version: Option[Long],
   writerClass: Option[Either[String, Class[DeltaManifestWriter]]]
 )

/**
 * Generate manifests from a Delta table
 */
case class GenerateManifestCommand(
    deltaLog: DeltaLog,
    options: Option[GenerateManifestOptions]
  ) extends DeltaCommand with ManifestWriterProvider{

  def run(sparkSession: SparkSession): Seq[DeltaManifest] = {
    val writer = createWriter(sparkSession, options)
    val snapshot = createSnapshot(deltaLog, options)
    val manifest = writer.write(deltaLog.fs, snapshot, options.flatMap(_.path))

    Seq(manifest)
  }

  private def createWriter(
    sparkSession: SparkSession,
    options: Option[GenerateManifestOptions]
  ): DeltaManifestWriter = options
    .flatMap(_.writerClass)
    .map(_ match {
      case Left(l) => createManifestWriter(l)
      case Right(r) => createManifestWriter(r)
    })
    .getOrElse(createManifestWriter(sparkSession))

  private def createSnapshot(
      deltaLog: DeltaLog,
      options: Option[GenerateManifestOptions]
  ): Snapshot = options
    .flatMap(_.version)
    .map(deltaLog.getSnapshotAt(_))
    .getOrElse(deltaLog.snapshot)
}
