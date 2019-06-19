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

package io.delta

import org.apache.spark.sql.delta._
import io.delta.execution._

import org.apache.spark.sql._

/**
 * Main class for programmatically interacting with Delta tables.
 * You can create DeltaTable instances using the static methods.
 * {{{
 *   DeltaTable.forPath(pathToTheDeltaTable)
 * }}}
 *
 */
class DeltaTable (df: Dataset[Row])
  extends DeltaTableOperations
  {

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   */
  def as(alias: String): DeltaTable = new DeltaTable(df.as(alias))

  /**
   * Get a DataFrame (that is, Dataset[Row]) representation of this Delta table.
   */
  def toDF: Dataset[Row] = df
}

object DeltaTable {
  /**
   * Create a DeltaTable for the data at the given `path`.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   */
  def forPath(path: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    forPath(sparkSession, path)
  }

  /**
   * Create a DeltaTable for the data at the given `path` using the given SparkSession to
   * read the data.
   */
  def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    new DeltaTable(sparkSession.read.format("delta").load(path))
  }

}
