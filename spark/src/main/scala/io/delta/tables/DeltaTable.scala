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

package io.delta.tables

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaTableUtils.withActiveSession
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{AlterTableDropFeatureDeltaCommand, AlterTableSetPropertiesDeltaCommand}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.tables.execution._
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

/**
 * Main class for programmatically interacting with Delta tables.
 * You can create DeltaTable instances using the static methods.
 * {{{
 *   DeltaTable.forPath(sparkSession, pathToTheDeltaTable)
 * }}}
 *
 * @since 0.3.0
 */
class DeltaTable private[tables](
    @transient private val _df: Dataset[Row],
    @transient private val table: DeltaTableV2)
  extends DeltaTableOperations with Serializable {

  protected def deltaLog: DeltaLog = {
    /** Assert the codes run in the driver. */
    if (table == null) {
      throw DeltaErrors.deltaTableFoundInExecutor()
    }

    table.deltaLog
  }

  protected def df: Dataset[Row] = {
    /** Assert the codes run in the driver. */
    if (_df == null) {
      throw DeltaErrors.deltaTableFoundInExecutor()
    }

    _df
  }

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @since 0.3.0
   */
  def as(alias: String): DeltaTable = new DeltaTable(df.as(alias), table)

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @since 0.3.0
   */
  def alias(alias: String): DeltaTable = as(alias)

  /**
   * Get a DataFrame (that is, Dataset[Row]) representation of this Delta table.
   *
   * @since 0.3.0
   */
  def toDF: Dataset[Row] = df

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * @param retentionHours The retention threshold in hours. Files required by the table for
   *                       reading versions earlier than this will be preserved and the
   *                       rest of them will be deleted.
   * @since 0.3.0
   */
  def vacuum(retentionHours: Double): DataFrame = {
    executeVacuum(table, Some(retentionHours))
  }

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * note: This will use the default retention period of 7 days.
   *
   * @since 0.3.0
   */
  def vacuum(): DataFrame = {
    executeVacuum(table, retentionHours = None)
  }

  /**
   * Get the information of the latest `limit` commits on this table as a Spark DataFrame.
   * The information is in reverse chronological order.
   *
   * @param limit The number of previous commands to get history for
   *
   * @since 0.3.0
   */
  def history(limit: Int): DataFrame = {
    executeHistory(deltaLog, Some(limit), table.catalogTable)
  }

  /**
   * Get the information available commits on this table as a Spark DataFrame.
   * The information is in reverse chronological order.
   *
   * @since 0.3.0
   */
  def history(): DataFrame = {
    executeHistory(deltaLog, catalogTable = table.catalogTable)
  }

  /**
   * :: Evolving ::
   *
   * Get the details of a Delta table such as the format, name, and size.
   *
   * @since 2.1.0
   */
  @Evolving
  def detail(): DataFrame = {
    executeDetails(deltaLog.dataPath.toString, table.getTableIdentifierIfExists)
  }

  /**
   * Generate a manifest for the given Delta Table
   *
   * @param mode Specifies the mode for the generation of the manifest.
   *             The valid modes are as follows (not case sensitive):
   *              - "symlink_format_manifest" : This will generate manifests in symlink format
   *                                            for Presto and Athena read support.
   *             See the online documentation for more information.
   * @since 0.5.0
   */
  def generate(mode: String): Unit = {
    executeGenerate(deltaLog.dataPath.toString, table.getTableIdentifierIfExists, mode)
  }

  /**
   * Delete data from the table that match the given `condition`.
   *
   * @param condition Boolean SQL expression
   *
   * @since 0.3.0
   */
  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  /**
   * Delete data from the table that match the given `condition`.
   *
   * @param condition Boolean SQL expression
   *
   * @since 0.3.0
   */
  def delete(condition: Column): Unit = {
    executeDelete(Some(condition.expr))
  }

  /**
   * Delete data from the table.
   *
   * @since 0.3.0
   */
  def delete(): Unit = {
    executeDelete(None)
  }

  /**
   * Optimize the data layout of the table. This returns
   * a [[DeltaOptimizeBuilder]] object that can be used to specify
   * the partition filter to limit the scope of optimize and
   * also execute different optimization techniques such as file
   * compaction or order data using Z-Order curves.
   *
   * See the [[DeltaOptimizeBuilder]] for a full description
   * of this operation.
   *
   * Scala example to run file compaction on a subset of
   * partitions in the table:
   * {{{
   *    deltaTable
   *     .optimize()
   *     .where("date='2021-11-18'")
   *     .executeCompaction();
   * }}}
   *
   * @since 2.0.0
   */
  def optimize(): DeltaOptimizeBuilder = DeltaOptimizeBuilder(table)

  /**
   * Update rows in the table based on the rules defined by `set`.
   *
   * Scala example to increment the column `data`.
   * {{{
   *    import org.apache.spark.sql.functions._
   *
   *    deltaTable.update(Map("data" -> col("data") + 1))
   * }}}
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 0.3.0
   */
  def update(set: Map[String, Column]): Unit = {
    executeUpdate(set, None)
  }

  /**
   * Update rows in the table based on the rules defined by `set`.
   *
   * Java example to increment the column `data`.
   * {{{
   *    import org.apache.spark.sql.Column;
   *    import org.apache.spark.sql.functions;
   *
   *    deltaTable.update(
   *      new HashMap<String, Column>() {{
   *        put("data", functions.col("data").plus(1));
   *      }}
   *    );
   * }}}
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 0.3.0
   */
  def update(set: java.util.Map[String, Column]): Unit = {
    executeUpdate(set.asScala, None)
  }

  /**
   * Update data from the table on the rows that match the given `condition`
   * based on the rules defined by `set`.
   *
   * Scala example to increment the column `data`.
   * {{{
   *    import org.apache.spark.sql.functions._
   *
   *    deltaTable.update(
   *      col("date") > "2018-01-01",
   *      Map("data" -> col("data") + 1))
   * }}}
   *
   * @param condition boolean expression as Column object specifying which rows to update.
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 0.3.0
   */
  def update(condition: Column, set: Map[String, Column]): Unit = {
    executeUpdate(set, Some(condition))
  }

  /**
   * Update data from the table on the rows that match the given `condition`
   * based on the rules defined by `set`.
   *
   * Java example to increment the column `data`.
   * {{{
   *    import org.apache.spark.sql.Column;
   *    import org.apache.spark.sql.functions;
   *
   *    deltaTable.update(
   *      functions.col("date").gt("2018-01-01"),
   *      new HashMap<String, Column>() {{
   *        put("data", functions.col("data").plus(1));
   *      }}
   *    );
   * }}}
   *
   * @param condition boolean expression as Column object specifying which rows to update.
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding update expressions as Column objects.
   * @since 0.3.0
   */
  def update(condition: Column, set: java.util.Map[String, Column]): Unit = {
    executeUpdate(set.asScala, Some(condition))
  }

  /**
   * Update rows in the table based on the rules defined by `set`.
   *
   * Scala example to increment the column `data`.
   * {{{
   *    deltaTable.updateExpr(Map("data" -> "data + 1")))
   * }}}
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(set: Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set), None)
  }

  /**
   * Update rows in the table based on the rules defined by `set`.
   *
   * Java example to increment the column `data`.
   * {{{
   *    deltaTable.updateExpr(
   *      new HashMap<String, String>() {{
   *        put("data", "data + 1");
   *      }}
   *    );
   * }}}
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(set: java.util.Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set.asScala), None)
  }

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   *
   * Scala example to increment the column `data`.
   * {{{
   *    deltaTable.update(
   *      "date > '2018-01-01'",
   *      Map("data" -> "data + 1"))
   * }}}
   *
   * @param condition boolean expression as SQL formatted string object specifying
   *                  which rows to update.
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(condition: String, set: Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set), Some(functions.expr(condition)))
  }

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   *
   * Java example to increment the column `data`.
   * {{{
   *    deltaTable.update(
   *      "date > '2018-01-01'",
   *      new HashMap<String, String>() {{
   *        put("data", "data + 1");
   *      }}
   *    );
   * }}}
   *
   * @param condition boolean expression as SQL formatted string object specifying
   *                  which rows to update.
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   * @since 0.3.0
   */
  def updateExpr(condition: String, set: java.util.Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set.asScala), Some(functions.expr(condition)))
  }

  /**
   * Merge data from the `source` DataFrame based on the given merge `condition`. This returns
   * a [[DeltaMergeBuilder]] object that can be used to specify the update, delete, or insert
   * actions to be performed on rows based on whether the rows matched the condition or not.
   *
   * See the [[DeltaMergeBuilder]] for a full description of this operation and what combinations of
   * update, delete and insert operations are allowed.
   *
   * Scala example to update a key-value Delta table with new key-values from a source DataFrame:
   * {{{
   *    deltaTable
   *     .as("target")
   *     .merge(
   *       source.as("source"),
   *       "target.key = source.key")
   *     .whenMatched
   *     .updateExpr(Map(
   *       "value" -> "source.value"))
   *     .whenNotMatched
   *     .insertExpr(Map(
   *       "key" -> "source.key",
   *       "value" -> "source.value"))
   *     .execute()
   * }}}
   *
   * Java example to update a key-value Delta table with new key-values from a source DataFrame:
   * {{{
   *    deltaTable
   *     .as("target")
   *     .merge(
   *       source.as("source"),
   *       "target.key = source.key")
   *     .whenMatched
   *     .updateExpr(
   *        new HashMap<String, String>() {{
   *          put("value" -> "source.value");
   *        }})
   *     .whenNotMatched
   *     .insertExpr(
   *        new HashMap<String, String>() {{
   *         put("key", "source.key");
   *         put("value", "source.value");
   *       }})
   *     .execute();
   * }}}
   *
   * @param source source Dataframe to be merged.
   * @param condition boolean expression as SQL formatted string
   * @since 0.3.0
   */
  def merge(source: DataFrame, condition: String): DeltaMergeBuilder = {
    merge(source, functions.expr(condition))
  }

  /**
   * Merge data from the `source` DataFrame based on the given merge `condition`. This returns
   * a [[DeltaMergeBuilder]] object that can be used to specify the update, delete, or insert
   * actions to be performed on rows based on whether the rows matched the condition or not.
   *
   * See the [[DeltaMergeBuilder]] for a full description of this operation and what combinations of
   * update, delete and insert operations are allowed.
   *
   * Scala example to update a key-value Delta table with new key-values from a source DataFrame:
   * {{{
   *    deltaTable
   *     .as("target")
   *     .merge(
   *       source.as("source"),
   *       "target.key = source.key")
   *     .whenMatched
   *     .updateExpr(Map(
   *       "value" -> "source.value"))
   *     .whenNotMatched
   *     .insertExpr(Map(
   *       "key" -> "source.key",
   *       "value" -> "source.value"))
   *     .execute()
   * }}}
   *
   * Java example to update a key-value Delta table with new key-values from a source DataFrame:
   * {{{
   *    deltaTable
   *     .as("target")
   *     .merge(
   *       source.as("source"),
   *       "target.key = source.key")
   *     .whenMatched
   *     .updateExpr(
   *        new HashMap<String, String>() {{
   *          put("value" -> "source.value")
   *        }})
   *     .whenNotMatched
   *     .insertExpr(
   *        new HashMap<String, String>() {{
   *         put("key", "source.key");
   *         put("value", "source.value");
   *       }})
   *     .execute()
   * }}}
   *
   * @param source source Dataframe to be merged.
   * @param condition boolean expression as a Column object
   * @since 0.3.0
   */
  def merge(source: DataFrame, condition: Column): DeltaMergeBuilder = {
    DeltaMergeBuilder(this, source, condition)
  }

  /**
   * Restore the DeltaTable to an older version of the table specified by version number.
   *
   * An example would be
   * {{{ io.delta.tables.DeltaTable.restoreToVersion(7) }}}
   *
   * @since 1.2.0
   */
  def restoreToVersion(version: Long): DataFrame = {
    executeRestore(table, Some(version), None)
  }

  /**
   * Restore the DeltaTable to an older version of the table specified by a timestamp.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss
   *
   * An example would be
   * {{{ io.delta.tables.DeltaTable.restoreToTimestamp("2019-01-01") }}}
   *
   * @since 1.2.0
   */
  def restoreToTimestamp(timestamp: String): DataFrame = {
    executeRestore(table, None, Some(timestamp))
  }

  /**
   * Updates the protocol version of the table to leverage new features. Upgrading the reader
   * version will prevent all clients that have an older version of Delta Lake from accessing this
   * table. Upgrading the writer version will prevent older versions of Delta Lake to write to this
   * table. The reader or writer version cannot be downgraded.
   *
   * See online documentation and Delta's protocol specification at PROTOCOL.md for more details.
   *
   * @since 0.8.0
   */
  def upgradeTableProtocol(readerVersion: Int, writerVersion: Int): Unit =
    withActiveSession(sparkSession) {
      val alterTableCmd = AlterTableSetPropertiesDeltaCommand(
        table,
        DeltaConfigs.validateConfigurations(
          Map(
            "delta.minReaderVersion" -> readerVersion.toString,
            "delta.minWriterVersion" -> writerVersion.toString)))
      toDataset(sparkSession, alterTableCmd)
    }

  /**
   * Modify the protocol to add a supported feature, and if the table does not support table
   * features, upgrade the protocol automatically. In such a case when the provided feature is
   * writer-only, the table's writer version will be upgraded to `7`, and when the provided
   * feature is reader-writer, both reader and writer versions will be upgraded, to `(3, 7)`.
   *
   * See online documentation and Delta's protocol specification at PROTOCOL.md for more details.
   *
   * @since 2.3.0
   */
  def addFeatureSupport(featureName: String): Unit = withActiveSession(sparkSession) {
    // Do not check for the correctness of the provided feature name. The ALTER TABLE command will
    // do that in a transaction.
    val alterTableCmd = AlterTableSetPropertiesDeltaCommand(
      table,
      Map(
        TableFeatureProtocolUtils.propertyKey(featureName) ->
          TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED))
    toDataset(sparkSession, alterTableCmd)
  }

  private def executeDropFeature(featureName: String, truncateHistory: Option[Boolean]): Unit = {
    val alterTableCmd = AlterTableDropFeatureDeltaCommand(
      table = table,
      featureName = featureName,
      truncateHistory = truncateHistory.getOrElse(false))
    toDataset(sparkSession, alterTableCmd)
  }

  /**
   * Modify the protocol to drop a supported feature. The operation always normalizes the
   * resulting protocol. Protocol normalization is the process of converting a table features
   * protocol to the weakest possible form. This primarily refers to converting a table features
   * protocol to a legacy protocol. A table features protocol can be represented with the legacy
   * representation only when the feature set of the former exactly matches a legacy protocol.
   * Normalization can also decrease the reader version of a table features protocol when it is
   * higher than necessary. For example:
   *
   * (1, 7, None, {AppendOnly, Invariants, CheckConstraints}) -> (1, 3)
   * (3, 7, None, {RowTracking}) -> (1, 7, RowTracking)
   *
   * The dropFeatureSupport method can be used as follows:
   * {{{
   *   io.delta.tables.DeltaTable.dropFeatureSupport("rowTracking")
   * }}}
   *
   * See online documentation for more details.
   *
   * @param featureName The name of the feature to drop.
   * @param truncateHistory Whether to truncate history before downgrading the protocol.
   * @return None.
   * @since 3.4.0
   */
  def dropFeatureSupport(
      featureName: String,
      truncateHistory: Boolean): Unit = withActiveSession(sparkSession) {
    executeDropFeature(featureName, Some(truncateHistory))
  }

  /**
   * Modify the protocol to drop a supported feature. The operation always normalizes the
   * resulting protocol. Protocol normalization is the process of converting a table features
   * protocol to the weakest possible form. This primarily refers to converting a table features
   * protocol to a legacy protocol. A table features protocol can be represented with the legacy
   * representation only when the feature set of the former exactly matches a legacy protocol.
   * Normalization can also decrease the reader version of a table features protocol when it is
   * higher than necessary. For example:
   *
   * (1, 7, None, {AppendOnly, Invariants, CheckConstraints}) -> (1, 3)
   * (3, 7, None, {RowTracking}) -> (1, 7, RowTracking)
   *
   * The dropFeatureSupport method can be used as follows:
   * {{{
   *   io.delta.tables.DeltaTable.dropFeatureSupport("rowTracking")
   * }}}
   *
   * Note, this command will not truncate history.
   *
   * See online documentation for more details.
   *
   * @param featureName The name of the feature to drop.
   * @return None.
   * @since 3.4.0
   */
  def dropFeatureSupport(featureName: String): Unit = withActiveSession(sparkSession) {
    executeDropFeature(featureName, None)
  }

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   *
   * Specifying properties here means that the target will override any properties with the same key
   * in the source table with the user-defined properties.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.clone(
   *   "/some/path/to/table",
   *   true,
   *   Map("foo" -> "bar"))
   * }}}
   *
   * @param target The path or table name to create the clone.
   * @param replace Whether to replace the destination with the clone command.
   * @param properties The table properties to override in the clone.
   *
   * @since 3.3.0
   */
  def clone(target: String, replace: Boolean, properties: Map[String, String]): DeltaTable = {
    executeClone(table, target, replace, properties)
  }

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.clone("/some/path/to/table", true)
   * }}}
   *
   * @param target The path or table name to create the clone.
   * @param replace Whether to replace the destination with the clone command.
   *
   * @since 3.3.0
   */
  def clone(target: String, replace: Boolean): DeltaTable = {
    clone(target, replace, properties = Map.empty)
  }

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.clone("/some/path/to/table")
   * }}}
   *
   * @param target The path or table name to create the clone.
   *
   * @since 3.3.0
   */
  def clone(target: String): DeltaTable = {
    clone(target, replace = false)
  }

  /**
   * Clone a DeltaTable at a specific version to a given destination to mirror the existing
   * table's data and metadata at that version.
   *
   * Specifying properties here means that the target will override any properties with the same key
   * in the source table with the user-defined properties.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.cloneAtVersion(
   *   5,
   *   "/some/path/to/table",
   *   true,
   *   Map("foo" -> "bar"))
   * }}}
   *
   * @param version The version of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param replace Whether to replace the destination with the clone command.
   * @param properties The table properties to override in the clone.
   *
   * @since 3.3.0
   */
  def cloneAtVersion(
      version: Long,
      target: String,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable = {
    executeClone(table, target, replace, properties, versionAsOf = Some(version))
  }

  /**
   * Clone a DeltaTable at a specific version to a given destination to mirror the existing
   * table's data and metadata at that version.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.cloneAtVersion(5, "/some/path/to/table", true)
   * }}}
   *
   * @param version The version of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param replace Whether to replace the destination with the clone command.
   *
   * @since 3.3.0
   */
  def cloneAtVersion(version: Long, target: String, replace: Boolean): DeltaTable = {
    cloneAtVersion(version, target, replace, properties = Map.empty)
  }

  /**
   * Clone a DeltaTable at a specific version to a given destination to mirror the existing
   * table's data and metadata at that version.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.cloneAtVersion(5, "/some/path/to/table")
   * }}}
   *
   * @param version The version of this table to clone from.
   * @param target The path or table name to create the clone.
   *
   * @since 3.3.0
   */
  def cloneAtVersion(version: Long, target: String): DeltaTable = {
    cloneAtVersion(version, target, replace = false)
  }

   /**
   * Clone a DeltaTable at a specific timestamp to a given destination to mirror the existing
   * table's data and metadata at that timestamp.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.
   *
   * Specifying properties here means that the target will override any properties with the same key
   * in the source table with the user-defined properties.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.cloneAtTimestamp(
   *   "2019-01-01",
   *   "/some/path/to/table",
   *   true,
   *   Map("foo" -> "bar"))
   * }}}
   *
   * @param timestamp The timestamp of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param replace Whether to replace the destination with the clone command.
   * @param properties The table properties to override in the clone.
   *
   * @since 3.3.0
   */
  def cloneAtTimestamp(
      timestamp: String,
      target: String,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable = {
    executeClone(table, target, replace, properties, timestampAsOf = Some(timestamp))
  }

  /**
   * Clone a DeltaTable at a specific timestamp to a given destination to mirror the existing
   * table's data and metadata at that timestamp.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.cloneAtTimestamp("2019-01-01", "/some/path/to/table", true)
   * }}}
   *
   * @param timestamp The timestamp of this table to clone from.
   * @param target The path or table name to create the clone.
   * @param replace Whether to replace the destination with the clone command.
   *
   * @since 3.3.0
   */
  def cloneAtTimestamp(timestamp: String, target: String, replace: Boolean): DeltaTable = {
    cloneAtTimestamp(timestamp, target, replace, properties = Map.empty)
  }

  /**
   * Clone a DeltaTable at a specific timestamp to a given destination to mirror the existing
   * table's data and metadata at that timestamp.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.cloneAtTimestamp("2019-01-01", "/some/path/to/table")
   * }}}
   *
   * @param timestamp The timestamp of this table to clone from.
   * @param target The path or table name to create the clone.
   *
   * @since 3.3.0
   */
  def cloneAtTimestamp(timestamp: String, target: String): DeltaTable = {
    cloneAtTimestamp(timestamp, target, replace = false)
  }
}

/**
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
   * Create a DeltaTable from the given parquet table and partition schema.
   * Takes an existing parquet table and constructs a delta transaction log in the base path of
   * that table.
   *
   * Note: Any changes to the table during the conversion process may not result in a consistent
   * state at the end of the conversion. Users should stop any changes to the table before the
   * conversion is started.
   *
   * An example usage would be
   * {{{
   *  io.delta.tables.DeltaTable.convertToDelta(
   *   spark,
   *   "parquet.`/path`",
   *   new StructType().add(StructField("key1", LongType)).add(StructField("key2", StringType)))
   * }}}
   *
   * @since 0.4.0
   */
  def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: StructType): DeltaTable = {
    val tableId: TableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(identifier)
    DeltaConvert.executeConvert(spark, tableId, Some(partitionSchema), None)
  }

  /**
   * Create a DeltaTable from the given parquet table and partition schema.
   * Takes an existing parquet table and constructs a delta transaction log in the base path of
   * that table.
   *
   * Note: Any changes to the table during the conversion process may not result in a consistent
   * state at the end of the conversion. Users should stop any changes to the table before the
   * conversion is started.
   *
   * An example usage would be
   * {{{
   *  io.delta.tables.DeltaTable.convertToDelta(
   *   spark,
   *   "parquet.`/path`",
   *   "key1 long, key2 string")
   * }}}
   *
   * @since 0.4.0
   */
  def convertToDelta(
      spark: SparkSession,
      identifier: String,
      partitionSchema: String): DeltaTable = {
    val tableId: TableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(identifier)
    DeltaConvert.executeConvert(spark, tableId, Some(StructType.fromDDL(partitionSchema)), None)
  }

  /**
   * Create a DeltaTable from the given parquet table. Takes an existing parquet table and
   * constructs a delta transaction log in the base path of the table.
   *
   * Note: Any changes to the table during the conversion process may not result in a consistent
   * state at the end of the conversion. Users should stop any changes to the table before the
   * conversion is started.
   *
   * An Example would be
   * {{{
   *  io.delta.tables.DeltaTable.convertToDelta(
   *   spark,
   *   "parquet.`/path`"
   * }}}
   *
   * @since 0.4.0
   */
  def convertToDelta(
      spark: SparkSession,
      identifier: String): DeltaTable = {
    val tableId: TableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(identifier)
    DeltaConvert.executeConvert(spark, tableId, None, None)
  }

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 0.3.0
   */
  def forPath(path: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    forPath(sparkSession, path)
  }

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * @since 0.3.0
   */
  def forPath(sparkSession: SparkSession, path: String): DeltaTable = {
    forPath(sparkSession, path, Map.empty[String, String])
  }

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * @param hadoopConf Hadoop configuration starting with "fs." or "dfs." will be picked up
   *                    by `DeltaTable` to access the file system when executing queries.
   *                    Other configurations will not be allowed.
   *
   * {{{
   *   val hadoopConf = Map(
   *     "fs.s3a.access.key" -> "<access-key>",
   *     "fs.s3a.secret.key" -> "<secret-key>"
   *   )
   *   DeltaTable.forPath(spark, "/path/to/table", hadoopConf)
   * }}}
   * @since 2.2.0
   */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: scala.collection.Map[String, String]): DeltaTable = {
    // We only pass hadoopConf so that we won't pass any unsafe options to Delta.
    val badOptions = hadoopConf.filterKeys { k =>
      !DeltaTableUtils.validDeltaTableHadoopPrefixes.exists(k.startsWith)
    }.toMap
    if (!badOptions.isEmpty) {
      throw DeltaErrors.unsupportedDeltaTableForPathHadoopConf(badOptions)
    }
    val fileSystemOptions: Map[String, String] = hadoopConf.toMap
    val hdpPath = new Path(path)
    if (DeltaTableUtils.isDeltaTable(sparkSession, hdpPath, fileSystemOptions)) {
      new DeltaTable(sparkSession.read.format("delta").options(fileSystemOptions).load(path),
        DeltaTableV2(
          spark = sparkSession,
          path = hdpPath,
          options = fileSystemOptions))
    } else {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(path = Some(path)))
    }
  }

  /**
  * Java friendly API to instantiate a [[DeltaTable]] object representing the data at the given
  * path, If the given path is invalid (i.e. either no table exists or an existing table is not a
  * Delta table), it throws a `not a Delta table` error.
  *
  * @param hadoopConf Hadoop configuration starting with "fs." or "dfs." will be picked up
  *                    by `DeltaTable` to access the file system when executing queries.
  *                    Other configurations will be ignored.
  *
  * {{{
  *   val hadoopConf = Map(
  *     "fs.s3a.access.key" -> "<access-key>",
  *     "fs.s3a.secret.key", "<secret-key>"
  *   )
  *   DeltaTable.forPath(spark, "/path/to/table", hadoopConf)
  * }}}
  * @since 2.2.0
  */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: java.util.Map[String, String]): DeltaTable = {
    val fsOptions = hadoopConf.asScala.toMap
    forPath(sparkSession, path, fsOptions)
  }

  /**
   * Instantiate a [[DeltaTable]] object using the given table name. If the given
   * tableOrViewName is invalid (i.e. either no table exists or an existing table is not a
   * Delta table), it throws a `not a Delta table` error. Note: Passing a view name will also
   * result in this error as views are not supported.
   *
   * The given tableOrViewName can also be the absolute path of a delta datasource (i.e.
   * delta.`path`), If so, instantiate a [[DeltaTable]] object representing the data at
   * the given path (consistent with the [[forPath]]).
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   */
  def forName(tableOrViewName: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    forName(sparkSession, tableOrViewName)
  }

  /**
   * Instantiate a [[DeltaTable]] object using the given table name using the given
   * SparkSession. If the given tableName is invalid (i.e. either no table exists or an
   * existing table is not a Delta table), it throws a `not a Delta table` error. Note:
   * Passing a view name will also result in this error as views are not supported.
   *
   * The given tableName can also be the absolute path of a delta datasource (i.e.
   * delta.`path`), If so, instantiate a [[DeltaTable]] object representing the data at
   * the given path (consistent with the [[forPath]]).
   */
  def forName(sparkSession: SparkSession, tableName: String): DeltaTable = {
    val tableId = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    if (DeltaTableUtils.isDeltaTable(sparkSession, tableId)) {
      val tbl = sparkSession.sessionState.catalog.getTableMetadata(tableId)
      new DeltaTable(
        sparkSession.table(tableName),
        DeltaTableV2(sparkSession, new Path(tbl.location), Some(tbl), Some(tableName)))
    } else if (DeltaTableUtils.isValidPath(tableId)) {
      forPath(sparkSession, tableId.table)
    } else {
      throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(table = Some(tableId)))
    }
  }

  /**
   * Check if the provided `identifier` string, in this case a file path,
   * is the root of a Delta table using the given SparkSession.
   *
   * An example would be
   * {{{
   *   DeltaTable.isDeltaTable(spark, "path/to/table")
   * }}}
   *
   * @since 0.4.0
   */
  def isDeltaTable(sparkSession: SparkSession, identifier: String): Boolean = {
    val identifierPath = new Path(identifier)
    if (sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_STRICT_CHECK_DELTA_TABLE)) {
      val rootOption = DeltaTableUtils.findDeltaTableRoot(sparkSession, identifierPath)
      rootOption.isDefined && DeltaLog.forTable(sparkSession, rootOption.get).tableExists
    } else {
      DeltaTableUtils.isDeltaTable(sparkSession, identifierPath)
    }
  }

  /**
   * Check if the provided `identifier` string, in this case a file path,
   * is the root of a Delta table.
   *
   * Note: This uses the active SparkSession in the current thread to search for the table. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * An example would be
   * {{{
   *   DeltaTable.isDeltaTable(spark, "/path/to/table")
   * }}}
   *
   * @since 0.4.0
   */
  def isDeltaTable(identifier: String): Boolean = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    isDeltaTable(sparkSession, identifier)
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to create a Delta table,
   * error if the table exists (the same as SQL `CREATE TABLE`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 1.0.0
   */
  @Evolving
  def create(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    create(sparkSession)
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to create a Delta table,
   * error if the table exists (the same as SQL `CREATE TABLE`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * @param spark sparkSession sparkSession passed by the user
   * @since 1.0.0
   */
  @Evolving
  def create(spark: SparkSession): DeltaTableBuilder = {
    new DeltaTableBuilder(spark, CreateTableOptions(ifNotExists = false))
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to create a Delta table,
   * if it does not exists (the same as SQL `CREATE TABLE IF NOT EXISTS`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 1.0.0
   */
  @Evolving
  def createIfNotExists(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    createIfNotExists(sparkSession)
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to create a Delta table,
   * if it does not exists (the same as SQL `CREATE TABLE IF NOT EXISTS`).
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * @param spark sparkSession sparkSession passed by the user
   * @since 1.0.0
   */
  @Evolving
  def createIfNotExists(spark: SparkSession): DeltaTableBuilder = {
    new DeltaTableBuilder(spark, CreateTableOptions(ifNotExists = true))
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to replace a Delta table,
   * error if the table doesn't exist (the same as SQL `REPLACE TABLE`)
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 1.0.0
   */
  @Evolving
  def replace(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    replace(sparkSession)
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to replace a Delta table,
   * error if the table doesn't exist (the same as SQL `REPLACE TABLE`)
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * @param spark sparkSession sparkSession passed by the user
   * @since 1.0.0
   */
  @Evolving
  def replace(spark: SparkSession): DeltaTableBuilder = {
    new DeltaTableBuilder(spark, ReplaceTableOptions(orCreate = false))
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to replace a Delta table
   * or create table if not exists (the same as SQL `CREATE OR REPLACE TABLE`)
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 1.0.0
   */
  @Evolving
  def createOrReplace(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    createOrReplace(sparkSession)
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaTableBuilder]] to replace a Delta table,
   * or create table if not exists (the same as SQL `CREATE OR REPLACE TABLE`)
   * Refer to [[DeltaTableBuilder]] for more details.
   *
   * @param spark sparkSession sparkSession passed by the user.
   * @since 1.0.0
   */
  @Evolving
  def createOrReplace(spark: SparkSession): DeltaTableBuilder = {
    new DeltaTableBuilder(spark, ReplaceTableOptions(orCreate = true))
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaColumnBuilder]] to specify a column.
   * Refer to [[DeltaTableBuilder]] for examples and [[DeltaColumnBuilder]] detailed APIs.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @param colName string the column name
   * @since 1.0.0
   */
  @Evolving
  def columnBuilder(colName: String): DeltaColumnBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw DeltaErrors.activeSparkSessionNotFound()
    }
    columnBuilder(sparkSession, colName)
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaColumnBuilder]] to specify a column.
   * Refer to [[DeltaTableBuilder]] for examples and [[DeltaColumnBuilder]] detailed APIs.
   *
   * @param spark sparkSession sparkSession passed by the user
   * @param colName string the column name
   * @since 1.0.0
   */
  @Evolving
  def columnBuilder(spark: SparkSession, colName: String): DeltaColumnBuilder = {
    new DeltaColumnBuilder(spark, colName)
  }
}
