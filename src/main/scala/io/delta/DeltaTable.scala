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
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.InterfaceStability._
import org.apache.spark.sql._

/**
 * :: Evolving ::
 *
 * Main class for programmatically interacting with Delta tables.
 * You can create DeltaTable instances using the static methods.
 * {{{
 *   DeltaTable.forPath(sparkSession, pathToTheDeltaTable)
 * }}}
 *
 * @since 0.3.0
 */
class DeltaTable (df: Dataset[Row]) extends DeltaTableOperations {

  /**
   * :: Evolving ::
   *
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @since 0.3.0
   */
  @Evolving
  def as(alias: String): DeltaTable = new DeltaTable(df.as(alias))

  /**
   * :: Evolving ::
   *
   * Get a DataFrame (that is, Dataset[Row]) representation of this Delta table.
   *
   * @since 0.3.0
   */
  @Evolving
  def toDF: Dataset[Row] = df

  /**
   * :: Evolving ::
   *
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold.
   *
   *
   * @param retentionHours The retention threshold in hours. Files required by the table for
   *                       reading versions earlier than this will be preserved and the
   *                       rest of them will be deleted.
   * @since 0.3.0
   */
  @Evolving
  def vacuum(retentionHours: Double): DataFrame = {
    executeVacuum(deltaLog, Some(retentionHours))
  }

  /**
   * :: Evolving ::
   *
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold.
   *
   * note: This will use the default retention period of 7 hours.
   *
   * @since 0.3.0
   */
  @Evolving
  def vacuum(): DataFrame = {
    executeVacuum(deltaLog, None)
  }

  /**
   * :: Evolving ::
   *
   * Delete data from the table that match the given `condition`.
   *
   * @param condition Boolean SQL expression
   *
   * @since 0.3.0
   */
  @Evolving
  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  /**
   * :: Evolving ::
   *
   * Delete data from the table that match the given `condition`.
   *
   * @param condition Boolean SQL expression
   *
   * @since 0.3.0
   */
  @Evolving
  def delete(condition: Column): Unit = {
    executeDelete(Some(condition.expr))
  }

  /**
   * :: Evolving ::
   *
   * Delete data from the table.
   *
   * @since 0.3.0
   */
  @Evolving
  def delete(): Unit = {
    executeDelete(None)
  }

  /**
   * :: Evolving ::
   *
   * Merge data from the `source` table that match the given `condition`
   *
   * @param source source Dataframe to be merged.
   * @param condition Boolean SQL expression
   * @return
   *
   * @since 0.3.0
   */
  @Evolving
  def merge(source: DataFrame, condition: String): DeltaMergeBuilder = {
    merge(source, functions.expr(condition))
  }

  /**
   * :: Evolving ::
   *
   * Merge data from the `source` table that match the given `condition`
   *
   * @param source source Dataframe to be merged.
   * @param condition Boolean SQL expression
   *
   * @since 0.3.0
   */
  @Evolving
  def merge(source: DataFrame, condition: Column): DeltaMergeBuilder = {
    DeltaMergeBuilder(this, source, condition, Nil)
  }
}

/**
 * :: Evolving ::
 *
 * Companion object to create DeltaTable instances.
 *
 * {{{
 *   DeltaTable.forPath(sparkSession, pathToTheDeltaTable)
 * }}}
 *
 * @since 0.3.0
 */
object DeltaTable {
  /**
   * :: Evolving ::
   *
   * Create a DeltaTable for the data at the given `path`.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 0.3.0
   */
  @Evolving
  def forPath(path: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    forPath(sparkSession, path)
  }

  /**
   * :: Evolving ::
   *
   * Create a DeltaTable for the data at the given `path` using the given SparkSession to
   * read the data.
   *
   * @since 0.3.0
   */
  @Evolving
  def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    if (DeltaTableUtils.isDeltaTable(sparkSession, new Path(path))) {
      new DeltaTable(sparkSession.read.format("delta").load(path))
    } else {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(path = Some(path)))
    }
  }

}
