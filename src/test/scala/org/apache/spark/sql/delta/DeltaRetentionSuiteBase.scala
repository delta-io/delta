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

package org.apache.spark.sql.delta

import java.io.File

import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.unsafe.types.CalendarInterval

trait DeltaRetentionSuiteBase extends QueryTest
  with SharedSparkSession {
  protected val testOp = Truncate()

  protected override def sparkConf: SparkConf = super.sparkConf
    // Disable the log cleanup because it runs asynchronously and causes test flakiness
    .set("spark.databricks.delta.properties.defaults.enableExpiredLogCleanup", "false")

  protected def intervalStringToMillis(str: String): Long = {
    CalendarInterval.fromString(str).milliseconds()
  }

  protected def getDeltaFiles(dir: File): Seq[File] =
    dir.listFiles().filter(_.getName.endsWith(".json"))

  protected def getCheckpointFiles(dir: File): Seq[File] =
    dir.listFiles().filter(f => FileNames.isCheckpointFile(new Path(f.getCanonicalPath)))

  protected def getLogFiles(dir: File): Seq[File]
}
