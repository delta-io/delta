/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import io.delta.connect.proto
import io.delta.tables.execution.{CreateTableOptions, ReplaceTableOptions}

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.{functions, Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.PrimitiveBooleanEncoder

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
    private val df: Dataset[Row],
    private val table: proto.DeltaTable)
  extends Serializable {

  private def sparkSession: SparkSession = df.sparkSession

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @since 2.4.0
   */
  def as(alias: String): DeltaTable = new DeltaTable(df.as(alias), table)

  /**
   * Apply an alias to the DeltaTable. This is similar to `Dataset.as(alias)` or
   * SQL `tableName AS alias`.
   *
   * @since 2.4.0
   */
  def alias(alias: String): DeltaTable = as(alias)

  /**
   * Get a DataFrame (that is, Dataset[Row]) representation of this Delta table.
   *
   * @since 2.4.0
   */
  def toDF: Dataset[Row] = df

  private def executeVacuum(retentionHours: Option[Double]): DataFrame = {
    val vacuum = proto.VacuumTable
      .newBuilder()
      .setTable(table)
    retentionHours.foreach(vacuum.setRetentionHours)
    val command = proto.DeltaCommand
      .newBuilder()
      .setVacuumTable(vacuum)
      .build()
    sparkSession.execute(com.google.protobuf.Any.pack(command).toByteArray)
    sparkSession.emptyDataFrame
  }

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * @param retentionHours The retention threshold in hours. Files required by the table for
   *                       reading versions earlier than this will be preserved and the
   *                       rest of them will be deleted.
   * @since 2.4.0
   */
  def vacuum(retentionHours: Double): DataFrame = {
    executeVacuum(Some(retentionHours))
  }

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * note: This will use the default retention period of 7 days.
   *
   * @since 2.4.0
   */
  def vacuum(): DataFrame = {
    executeVacuum(None)
  }

  private def executeHistory(limit: Option[Int]): DataFrame = {
    val describeHistory = proto.DescribeHistory
      .newBuilder()
      .setTable(table)
    val relation = proto.DeltaRelation.newBuilder().setDescribeHistory(describeHistory).build()
    val extension = com.google.protobuf.Any.pack(relation).toByteArray
    val df = sparkSession.newDataFrame(extension)
    limit match {
      case Some(limit) => df.limit(limit)
      case None => df
    }
  }

  /**
   * Get the information of the latest `limit` commits on this table as a Spark DataFrame.
   * The information is in reverse chronological order.
   *
   * @param limit The number of previous commands to get history for
   * @since 2.4.0
   */
  def history(limit: Int): DataFrame = {
    executeHistory(Some(limit))
  }

  /**
   * Get the information available commits on this table as a Spark DataFrame.
   * The information is in reverse chronological order.
   *
   * @since 2.4.0
   */
  def history(): DataFrame = {
    executeHistory(limit = None)
  }

  /**
   * :: Evolving ::
   *
   * Get the details of a Delta table such as the format, name, and size.
   *
   * @since 2.4.0
   */
  @Evolving
  def detail(): DataFrame = {
    val describeDetail = proto.DescribeDetail
      .newBuilder()
      .setTable(table)
    val relation = proto.DeltaRelation.newBuilder().setDescribeDetail(describeDetail).build()
    val extension = com.google.protobuf.Any.pack(relation).toByteArray
    sparkSession.newDataFrame(extension)
  }

  /**
   * Generate a manifest for the given Delta Table
   *
   * @param mode Specifies the mode for the generation of the manifest.
   *             The valid modes are as follows (not case sensitive):
   *              - "symlink_format_manifest" : This will generate manifests in symlink format
   *                for Presto and Athena read support.
   *                See the online documentation for more information.
   * @since 2.5.0
   */
  def generate(mode: String): Unit = {
    val generate = proto.Generate
      .newBuilder()
      .setTable(table)
      .setMode(mode)
    val command = proto.DeltaCommand.newBuilder().setGenerate(generate).build()
    val extension = com.google.protobuf.Any.pack(command).toByteArray
    sparkSession.execute(extension)
  }

  private def executeDelete(condition: Option[Column]): Unit = {
    val delete = proto.DeleteFromTable
      .newBuilder()
      .setTarget(df.plan.getRoot)
    condition.foreach(c => delete.setCondition(c.expr))
    val relation = proto.DeltaRelation.newBuilder().setDeleteFromTable(delete).build()
    val extension = com.google.protobuf.Any.pack(relation).toByteArray
    sparkSession.newDataFrame(extension).collect()
  }

  /**
   * Delete data from the table that match the given `condition`.
   *
   * @param condition Boolean SQL expression
   * @since 2.5.0
   */
  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  /**
   * Delete data from the table that match the given `condition`.
   *
   * @param condition Boolean SQL expression
   * @since 2.5.0
   */
  def delete(condition: Column): Unit = {
    executeDelete(condition = Some(condition))
  }

  /**
   * Delete data from the table.
   *
   * @since 2.5.0
   */
  def delete(): Unit = {
    executeDelete(condition = None)
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
   * @since 2.5.0
   */
  def optimize(): DeltaOptimizeBuilder = {
    DeltaOptimizeBuilder(sparkSession, table)
  }

  private def executeUpdate(condition: Option[Column], set: Map[String, Column]): Unit = {
    val assignments = set.toSeq.map { case (field, value) =>
      proto.Assignment
        .newBuilder()
        .setField(functions.expr(field).expr)
        .setValue(value.expr)
        .build()
    }
    val update = proto.UpdateTable
      .newBuilder()
      .setTarget(df.plan.getRoot)
      .addAllAssignments(assignments.asJava)
    condition.foreach(c => update.setCondition(c.expr))
    val relation = proto.DeltaRelation.newBuilder().setUpdateTable(update).build()
    val extension = com.google.protobuf.Any.pack(relation).toByteArray
    sparkSession.newDataFrame(extension).collect()
  }

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
   * @since 2.5.0
   */
  def update(set: Map[String, Column]): Unit = {
    executeUpdate(condition = None, set)
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
   * @since 2.5.0
   */
  def update(set: java.util.Map[String, Column]): Unit = {
    update(set.asScala.asInstanceOf[Map[String, Column]])
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
   * @param set       rules to update a row as a Scala map between target column names and
   *                  corresponding update expressions as Column objects.
   * @since 2.5.0
   */
  def update(condition: Column, set: Map[String, Column]): Unit = {
    executeUpdate(Some(condition), set)
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
   * @param set       rules to update a row as a Java map between target column names and
   *                  corresponding update expressions as Column objects.
   * @since 2.5.0
   */
  def update(condition: Column, set: java.util.Map[String, Column]): Unit = {
    executeUpdate(Some(condition), set.asScala.toMap)
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
   * @since 2.5.0
   */
  def updateExpr(set: Map[String, String]): Unit = {
    update(toStrColumnMap(set))
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
   * @since 2.5.0
   */
  def updateExpr(set: java.util.Map[String, String]): Unit = {
    update(toStrColumnMap(set.asScala.toMap))
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
   * @param set       rules to update a row as a Scala map between target column names and
   *                  corresponding update expressions as SQL formatted strings.
   * @since 2.5.0
   */
  def updateExpr(condition: String, set: Map[String, String]): Unit = {
    executeUpdate(Some(functions.expr(condition)), toStrColumnMap(set))
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
   * @param set       rules to update a row as a Java map between target column names and
   *                  corresponding update expressions as SQL formatted strings.
   * @since 2.5.0
   */
  def updateExpr(condition: String, set: java.util.Map[String, String]): Unit = {
    executeUpdate(Some(functions.expr(condition)), toStrColumnMap(set.asScala.toMap))
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
   * @param source    source Dataframe to be merged.
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
   * @param source    source Dataframe to be merged.
   * @param condition boolean expression as a Column object
   * @since 0.3.0
   */
  def merge(source: DataFrame, condition: Column): DeltaMergeBuilder = {
    DeltaMergeBuilder(this, source, condition)
  }

  private def executeClone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      version: Option[Int],
      timestamp: Option[String],
      properties: Map[String, String]): DeltaTable = {
    val clone = proto.CloneTable
      .newBuilder()
      .setTable(table)
      .setTarget(target)
      .setIsShallow(isShallow)
      .setReplace(replace)
      .putAllProperties(properties.asJava)
    version.foreach(clone.setVersion)
    timestamp.foreach(clone.setTimestamp)
    val command = proto.DeltaCommand.newBuilder().setCloneTable(clone).build()
    val extension = com.google.protobuf.Any.pack(command).toByteArray
    sparkSession.execute(extension)
    DeltaTable.forPath(sparkSession, target)
  }

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   * Clones can be SHALLOW or DEEP. By default, clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * By setting the replace argument to true, if the target directory is non-empty, we will still
   * continue with the clone. Otherwise throw an error.
   *
   * Specifying properties here means that the target will override any properties with the same key
   * in the source table with the user-defined properties
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.clone(
   *   "/some/path",
   *   true,
   *   true,
   *   Map("foo" -> "bar"))
   * }}}
   */
  def clone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable = {
    executeClone(target, isShallow, replace, version = None, timestamp = None, properties)
  }

  /**
   * clone used by Python implementation using java.util.HashMap
   */
  def clone(
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable = {
    val scalaProps = Option(properties).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    clone(target, isShallow, replace, scalaProps)
  }

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   * Clones can be SHALLOW or DEEP. By default, clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * By setting the replace argument to true, if the target directory is non-empty, we will still
   * continue with the clone. Otherwise throw an error.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.clone(
   *   "/some/path",
   *   true,
   *   true)
   * }}}
   */
  def clone(target: String, isShallow: Boolean, replace: Boolean): DeltaTable = {
    clone(target, isShallow, replace, properties = Map.empty[String, String])
  }

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   * Clones can be SHALLOW or DEEP. By default, clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.clone(
   *   "/some/path",
   *   true)
   * }}}
   */
  def clone(target: String, isShallow: Boolean): DeltaTable = {
    clone(target, isShallow, replace = false)
  }

  /**
   * Clone a DeltaTable to a given destination to mirror the existing table's data and metadata.
   * Clones can be SHALLOW or DEEP. By default, clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * An example would be
   * {{{
   *  io.delta.tables.DeltaTable.clone("/some/path")
   * }}}
   */
  def clone(target: String): DeltaTable = {
    clone(target, isShallow = false)
  }

  /**
   * Clone a DeltaTable at the given version to a destination which mirrors the existing
   * table's data and metadata at that version.
   * Clones can be SHALLOW or DEEP. By default, clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * By setting the replace argument to true, if the target directory is non-empty, we will still
   * continue with the clone. Otherwise throw an error.
   *
   * Specifying properties here means that the target will override any properties with the same key
   * in the source table with the user-defined properties
   *
   * An example would be
   * {{{
   *   io.delta.tables.DeltaTable.cloneAtVersion(
   *     5,
   *     "/path/to/table",
   *     true,
   *     true,
   *     Map("foo" -> "bar"))
   * }}}
   */
  def cloneAtVersion(
      version: Int,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable = {
    executeClone(target, isShallow, replace, version = Some(version), timestamp = None, properties)
  }

  /**
   * cloneAtVersion used by Python implementation using java.util.HashMap
   */
  def cloneAtVersion(
      version: Int,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable = {
    val scalaProps = Option(properties).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    cloneAtVersion(version, target, isShallow, replace, scalaProps)
  }

  /**
   * Clone a DeltaTable at the given version to a destination which mirrors the existing
   * table's data and metadata at that version.
   * Clones can be SHALLOW or DEEP. By default, clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * By setting the replace argument to true, if the target directory is non-empty, we will still
   * continue with the clone. Otherwise throw an error.
   *
   * An example would be
   * {{{
   *   io.delta.tables.DeltaTable.cloneAtVersion(
   *     5,
   *     "/path/to/table",
   *     true,
   *     true)
   * }}}
   */
  def cloneAtVersion(
      version: Int, target: String, isShallow: Boolean, replace: Boolean): DeltaTable = {
    cloneAtVersion(version, target, isShallow, replace, properties = Map.empty[String, String])
  }

  /**
   * Clone a DeltaTable at the given version to a destination which mirrors the existing
   * table's data and metadata at that version.
   * Clones can be SHALLOW or DEEP. By default, clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * An example would be
   * {{{
   *   io.delta.tables.DeltaTable.cloneAtVersion(
   *     5,
   *     "/path/to/table",
   *     true)
   * }}}
   */
  def cloneAtVersion(version: Int, target: String, isShallow: Boolean): DeltaTable = {
    cloneAtVersion(version, target, isShallow, replace = false)
  }

  /**
   * Clone a DeltaTable at the given version to a destination which mirrors the existing
   * table's data and metadata at that version.
   * Clones can be SHALLOW or DEEP. By default, clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * An example would be
   * {{{
   *   io.delta.tables.DeltaTable.cloneAtVersion(
   *     5,
   *     "/path/to/table")
   * }}}
   */
  def cloneAtVersion(version: Int, target: String): DeltaTable = {
    cloneAtVersion(version, target, isShallow = false)
  }

  /**
   * Clone a DeltaTable at the given timestamp to a destination which mirrors the existing
   * table's data and metadata at that timestamp.
   * Clones can be SHALLOW or DEEP. By default clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss
   *
   * By setting the replace argument to true, if the target directory is non-empty, we will still
   * continue with the clone. Otherwise throw an error.
   *
   * Specifying properties here means that the target will override any properties with the same key
   * in the source table with the user-defined properties
   *
   * An example would be
   * {{{
   *
   * io.delta.tables.DeltaTable.cloneAtTimestamp(
   *     "2019-01-01",
   *     "/path/to/table",
   *     true,
   *     true,
   *     Map("foo" -> "bar"))
   * }}}
   */
  def cloneAtTimestamp(
      timestamp: String,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: Map[String, String]): DeltaTable = {
    executeClone(
      target, isShallow, replace, version = None, timestamp = Some(timestamp), properties)
  }

  /**
   * cloneAtTimestamp used by Python implementation using java.util.HashMap
   */
  def cloneAtTimestamp(
      timestamp: String,
      target: String,
      isShallow: Boolean,
      replace: Boolean,
      properties: java.util.HashMap[String, String]): DeltaTable = {
    val scalaProps = Option(properties).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    cloneAtTimestamp(timestamp, target, isShallow, replace, scalaProps)
  }

  /**
   * Clone a DeltaTable at the given timestamp to a destination which mirrors the existing
   * table's data and metadata at that timestamp.
   * Clones can be SHALLOW or DEEP. By default clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss
   *
   * By setting the replace argument to true, if the target directory is non-empty, we will still
   * continue with the clone. Otherwise throw an error.
   *
   * An example would be
   * {{{
   *
   * io.delta.tables.DeltaTable.cloneAtTimestamp(
   *     "2019-01-01",
   *     "/path/to/table",
   *     true,
   *     true)
   * }}}
   */
  def cloneAtTimestamp(
      timestamp: String, target: String, isShallow: Boolean, replace: Boolean): DeltaTable = {
    cloneAtTimestamp(timestamp, target, isShallow, replace, properties = Map.empty[String, String])
  }

  /**
   * Clone a DeltaTable at the given timestamp to a destination which mirrors the existing
   * table's data and metadata at that timestamp.
   * Clones can be SHALLOW or DEEP. By default clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss
   *
   * An example would be
   * {{{
   *
   * io.delta.tables.DeltaTable.cloneAtTimestamp(
   *     "2019-01-01",
   *     "/path/to/table",
   *     true)
   * }}}
   */
  def cloneAtTimestamp(timestamp: String, target: String, isShallow: Boolean): DeltaTable = {
    cloneAtTimestamp(timestamp, target, isShallow, replace = false)
  }

  /**
   * Clone a DeltaTable at the given timestamp to a destination which mirrors the existing
   * table's data and metadata at that timestamp.
   * Clones can be SHALLOW or DEEP. By default clones are DEEP.
   *
   * Shallow cloned tables contain only a delta transaction log in the cloned directory when
   * initially cloned. Data files in the transaction log point to the original cloned table. Any
   * data added to shallow clone only affects the shallow clone.
   *
   * Note: Since shallow clones point to files in the original table, deleting files in the
   * original table can break shallow clones.
   *
   * For deep clones, data files are copied to the destination directory as well. Deep clones do not
   * depend in any way on the original table from which they were cloned.
   *
   * Timestamp can be of the format yyyy-MM-dd or yyyy-MM-dd HH:mm:ss
   *
   * An example would be
   * {{{
   *
   * io.delta.tables.DeltaTable.cloneAtTimestamp(
   *     "2019-01-01",
   *     "/path/to/table",
   *     true)
   * }}}
   */
  def cloneAtTimestamp(timestamp: String, target: String): DeltaTable = {
    cloneAtTimestamp(timestamp, target, isShallow = false)
  }

  private def executeRestore(version: Option[Long], timestamp: Option[String]): DataFrame = {
    val restore = proto.RestoreTable
      .newBuilder()
      .setTable(table)
    version.foreach(restore.setVersion)
    timestamp.foreach(restore.setTimestamp)
    val relation = proto.DeltaRelation.newBuilder().setRestoreTable(restore).build()
    val extension = com.google.protobuf.Any.pack(relation).toByteArray
    val result = sparkSession.newDataFrame(extension).collectResult()
    sparkSession.createDataFrame(result.toArray.toSeq.asJava, result.schema)
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
    executeRestore(version = Some(version), timestamp = None)
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
    executeRestore(version = None, timestamp = Some(timestamp))
  }

  /**
   * Updates the protocol version of the table to leverage new features. Upgrading the reader
   * version will prevent all clients that have an older version of Delta Lake from accessing this
   * table. Upgrading the writer version will prevent older versions of Delta Lake to write to this
   * table. The reader or writer version cannot be downgraded.
   *
   * See online documentation and Delta's protocol specification at PROTOCOL.md for more details.
   *
   * @since 2.5.0
   */
  def upgradeTableProtocol(readerVersion: Int, writerVersion: Int): Unit = {
    val upgrade = proto.UpgradeTableProtocol
      .newBuilder()
      .setTable(table)
      .setReaderVersion(readerVersion)
      .setWriterVersion(writerVersion)
    val command = proto.DeltaCommand.newBuilder().setUpgradeTableProtocol(upgrade).build()
    val extension = com.google.protobuf.Any.pack(command).toByteArray
    sparkSession.execute(extension)
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }
}

/**
 * Companion object to create DeltaTable instances.
 *
 * {{{
 *   DeltaTable.forPath(sparkSession, pathToTheDeltaTable)
 * }}}
 *
 * @since 2.4.0
 */
object DeltaTable {
  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
   * this throws error if active SparkSession has not been set, that is,
   * `SparkSession.getActiveSession()` is empty.
   *
   * @since 2.4.0
   */
  def forPath(path: String): DeltaTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    forPath(sparkSession, path)
  }

  /**
   * Instantiate a [[DeltaTable]] object representing the data at the given path, If the given
   * path is invalid (i.e. either no table exists or an existing table is not a Delta table),
   * it throws a `not a Delta table` error.
   *
   * @since 2.4.0
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
   * @since 2.4.0
   */
  def forPath(
      sparkSession: SparkSession,
      path: String,
      hadoopConf: scala.collection.Map[String, String]): DeltaTable = {
    val table = proto.DeltaTable
      .newBuilder()
      .setPath(
        proto.DeltaTable.Path
          .newBuilder().setPath(path)
          .putAllHadoopConf(hadoopConf.asJava))
      .build()
    forTable(sparkSession, table)
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
   * @since 2.4.0
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
      throw new IllegalArgumentException("Could not find active SparkSession")
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
   *
   * @since 2.4.0
   */
  def forName(sparkSession: SparkSession, tableName: String): DeltaTable = {
    val table = proto.DeltaTable
      .newBuilder()
      .setTableOrViewName(tableName)
      .build()
    forTable(sparkSession, table)
  }

  private def forTable(sparkSession: SparkSession, table: proto.DeltaTable): DeltaTable = {
    val relation = proto.DeltaRelation
      .newBuilder()
      .setScan(proto.Scan.newBuilder().setTable(table))
      .build()
    val df = sparkSession.newDataFrame(com.google.protobuf.Any.pack(relation).toByteArray)
    new DeltaTable(df, table)
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
   * @since 2.4.0
   */
  def isDeltaTable(sparkSession: SparkSession, identifier: String): Boolean = {
    val relation = proto.DeltaRelation
      .newBuilder()
      .setIsDeltaTable(proto.IsDeltaTable.newBuilder().setPath(identifier))
      .build()
    val extension = com.google.protobuf.Any.pack(relation).toByteArray
    sparkSession.newDataset(extension, PrimitiveBooleanEncoder).head()
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
   * @since 2.4.0
   */
  def isDeltaTable(identifier: String): Boolean = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
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
   * @since 2.5.0
   */
  @Evolving
  def create(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
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
   * @since 2.5.0
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
   * @since 2.5.0
   */
  @Evolving
  def createIfNotExists(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
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
   * @since 2.5.0
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
   * @since 2.5.0
   */
  @Evolving
  def replace(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
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
   * @since 2.5.0
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
   * @since 2.5.0
   */
  @Evolving
  def createOrReplace(): DeltaTableBuilder = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
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
   * @since 2.5.0
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
   * @since 2.5.0
   */
  @Evolving
  def columnBuilder(colName: String): DeltaColumnBuilder = {
    new DeltaColumnBuilder(colName)
  }

  /**
   * :: Evolving ::
   *
   * Return an instance of [[DeltaColumnBuilder]] to specify a column.
   * Refer to [[DeltaTableBuilder]] for examples and [[DeltaColumnBuilder]] detailed APIs.
   *
   * @param spark   sparkSession sparkSession passed by the user
   * @param colName string the column name
   * @since 2.5.0
   */
  @Evolving
  def columnBuilder(spark: SparkSession, colName: String): DeltaColumnBuilder = {
    new DeltaColumnBuilder(colName)
  }
}
