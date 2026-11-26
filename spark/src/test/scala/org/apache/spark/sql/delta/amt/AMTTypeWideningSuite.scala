/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.amt

import java.util.Locale

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaConfigs, TimestampNTZTableFeature}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._

import org.apache.spark.sql.types.{DataType, StructType, TimestampNTZType}

/**
 * ALTER COLUMN TYPE on AMT tables, for each type change that Delta type widening supports. The
 * files and the tree written before the change keep the old type, so reads and the trees written
 * after the change must read them as the new type.
 *
 * Each test table has the columns `id INT`, `data_col` and `partition_col`, and is partitioned by
 * `partition_col`. Before the type change, it gets one file for each id 0 until `leafPackedFiles`,
 * and both columns are of `fromType`. After both columns are changed to `toType`, the commit under
 * test writes one more row, with id `leafPackedFiles` and a value that only `toType` can hold. For
 * example, for INT -> BIGINT, the table has these rows:
 * {{{
 *   id | data_col   | partition_col
 *    0 |          0 |             0  written as INT before the change, one row per file
 *   ...
 *   29 |         29 |            29
 *   30 | 3000000000 |    3000000000  written as BIGINT after the change
 * }}}
 */
class AMTTypeWideningSuite extends AMTCheckpointTestBase {

  /**
   * A type change that a test applies to both `data_col` and `partition_col`.
   *
   * @param fromType the type of both columns when the table is created.
   * @param toType the type that ALTER COLUMN TYPE changes both columns to.
   * @param generateInitialValue the SQL expression that gives the `fromType` value of both columns
   *                             in a row written before the change, from the row's `id` column.
   * @param newValue the value of both columns in the row written after the change. Only `toType`
   *                 can hold it.
   */
  private case class TypeChange(
      fromType: String,
      toType: String,
      generateInitialValue: String,
      newValue: String) {
    /** `value` cast to `toType`. */
    def castToType(value: String): String = s"CAST($value AS $toType)"

    /** The part of a table name that identifies this type change, e.g. `int_to_decimal_10_0`. */
    def tableNameSuffix: String =
      Seq(fromType, toType)
        .map(_.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_").stripSuffix("_"))
        .mkString("_to_")
  }

  private val typeChanges = Seq(
    TypeChange("TINYINT", "SMALLINT", "CAST(id AS TINYINT)", "30000"),
    TypeChange("TINYINT", "INT", "CAST(id AS TINYINT)", "2000000000"),
    TypeChange("TINYINT", "BIGINT", "CAST(id AS TINYINT)", "3000000000"),
    TypeChange("SMALLINT", "INT", "CAST(id AS SMALLINT)", "2000000000"),
    TypeChange("SMALLINT", "BIGINT", "CAST(id AS SMALLINT)", "3000000000"),
    TypeChange("INT", "BIGINT", "CAST(id AS INT)", "3000000000"),
    TypeChange("FLOAT", "DOUBLE", "CAST(id + 0.5 AS FLOAT)", "1.0E300"),
    TypeChange("DATE", "TIMESTAMP_NTZ", "date_add(DATE'2024-01-01', CAST(id AS INT))",
      "TIMESTAMP_NTZ'2030-05-05 12:34:56'"),
    TypeChange("DECIMAL(9, 2)", "DECIMAL(18, 2)", "CAST(id + 0.25 AS DECIMAL(9, 2))",
      "1234567890123456.25"),
    TypeChange("DECIMAL(8, 2)", "DECIMAL(9, 3)", "CAST(id + 0.25 AS DECIMAL(8, 2))",
      "123456.125"),
    TypeChange("DECIMAL(9, 2)", "DECIMAL(19, 3)", "CAST(id + 0.25 AS DECIMAL(9, 2))",
      "1234567890123456.125"),
    TypeChange("TINYINT", "DOUBLE", "CAST(id AS TINYINT)", "0.5"),
    TypeChange("SMALLINT", "DOUBLE", "CAST(id AS SMALLINT)", "0.5"),
    TypeChange("INT", "DOUBLE", "CAST(id AS INT)", "0.5"),
    TypeChange("TINYINT", "DECIMAL(10, 0)", "CAST(id AS TINYINT)", "9999999999"),
    TypeChange("SMALLINT", "DECIMAL(10, 0)", "CAST(id AS SMALLINT)", "9999999999"),
    TypeChange("INT", "DECIMAL(10, 0)", "CAST(id AS INT)", "9999999999"),
    TypeChange("BIGINT", "DECIMAL(20, 0)", "CAST(id AS BIGINT)", "99999999999999999999"))

  private def sqlConfs: Seq[(String, String)] = leafPackingConfs

  /** Registers a test of `change`, on the table described above, for each checkpoint scenario. */
  private def testTypeChange(change: TypeChange, behavior: String)(
      body: AMTCheckpointScenarioContext => Unit): Unit =
    testAcrossAMTCheckpointScenarios(
        s"ALTER COLUMN TYPE ${change.fromType} -> ${change.toType}: $behavior",
        s"amt_${change.tableNameSuffix}",
        sqlConfs = sqlConfs,
        tableSchema = s"id INT, data_col ${change.fromType}, partition_col ${change.fromType}",
        partitionColumns = Seq("partition_col"))(
        setup = name => {
          sql(s"ALTER TABLE $name SET TBLPROPERTIES (" +
            s"'${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'true', " +
            s"'${propertyKey(TimestampNTZTableFeature)}' = '$FEATURE_PROP_SUPPORTED')")
          appendRowsAsSeparateFiles(
            name,
            numFiles = leafPackedFiles,
            startId = 0,
            columnExprs = Seq(
              "CAST(id AS INT)",
              change.generateInitialValue,
              change.generateInitialValue))
          commitCheckpoint(deltaLogForName(name), incremental = false)
          Seq("data_col", "partition_col").foreach { column =>
            sql(s"ALTER TABLE $name ALTER COLUMN $column TYPE ${change.toType}")
          }
        },
        inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
          s"INSERT INTO $name VALUES ($leafPackedFiles, ${change.castToType(change.newValue)}, " +
            s"${change.castToType(change.newValue)})")))(body)

  typeChanges.foreach { change =>
    val fromType = DataType.fromDDL(change.fromType)
    val toType = DataType.fromDDL(change.toType)

    testTypeChange(change,
        s"rows written before the change read back as ${change.toType}, and new manifests " +
          s"store ${change.toType}") { context =>
      Seq("data_col", "partition_col").foreach { column =>
        val tableType = context.postCheckpointSnapshot.metadata.schema(column).dataType
        assert(tableType == toType, s"The table schema has $column as $tableType, not $toType")
        val treeType = context.checkpoint.metaData.schema(column).dataType
        assert(treeType == toType, s"The tree's metadata has $column as $treeType, not $toType")
      }

      // Reading the table must return each row with its values cast to `toType`. The expected
      // rows are built from the same value expressions with SQL CAST, without reading the table.
      // For INT -> BIGINT, they are (0, 0L, 0L), ..., (29, 29L, 29L) and
      // (30, 3000000000L, 3000000000L).
      val initialValue = change.castToType(change.generateInitialValue)
      val newValue = change.castToType(change.newValue)
      checkAnswer(
        spark.table(context.tableName),
        sql(
          s"""SELECT CAST(id AS INT), $initialValue, $initialValue
             |FROM range(0, $leafPackedFiles)
             |UNION ALL SELECT $leafPackedFiles, $newValue, $newValue""".stripMargin))

      // A full rewrite writes every manifest in `toType`. An incremental tree writes its root in
      // `toType` and keeps the leaves written before the change, in `fromType`.
      val metadata = context.postCheckpointSnapshot.metadata
      val dataColStats = s"data_col_${DeltaColumnMapping.getColumnId(metadata.schema("data_col"))}"
      // The type of the `data_col` lower bound, if any, and of `partition_col` in a manifest.
      def getTypeInStats(path: String): (Option[DataType], DataType) = allowReadWithinDeltaLog {
        val schema = spark.read.parquet(path).schema
        val boundTypes = schema("content_stats").dataType.asInstanceOf[StructType](dataColStats)
          .dataType.asInstanceOf[StructType]
        (boundTypes.find(_.name == "lower_bound").map(_.dataType),
          schema("partition").dataType.asInstanceOf[StructType]("partition_col").dataType)
      }
      // The value of `getTypeInStats` for a manifest written in `dataType`. AMT keeps no bounds
      // for timestamps.
      def typesInStats(dataType: DataType): (Option[DataType], DataType) = dataType match {
        case TimestampNTZType => (None, dataType)
        case _ => (Some(dataType), dataType)
      }
      val inToType = typesInStats(toType)
      val inFromType = typesInStats(fromType)
      context.provider.topLevelFiles.foreach { root =>
        assert(getTypeInStats(root.getPath.toString) == inToType)
      }
      val leaves = context.provider.liveLeafManifestAbsolutePaths
      assert(leaves.nonEmpty)
      leaves.foreach { leaf =>
        val expected = if (context.scenario.isIncremental) inFromType else inToType
        assert(getTypeInStats(leaf.toString) == expected)
      }
    }
  }
}
