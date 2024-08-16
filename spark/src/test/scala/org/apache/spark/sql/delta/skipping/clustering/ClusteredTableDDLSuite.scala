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

package org.apache.spark.sql.delta.skipping.clustering

import java.io.File

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtils
import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaColumnMappingEnableIdMode, DeltaColumnMappingEnableNameMode, DeltaConfigs, DeltaExcludedBySparkVersionTestMixinShims, DeltaLog, DeltaUnsupportedOperationException}
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.hooks.UpdateCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.SkippingEligibleDataType
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}

trait ClusteredTableCreateOrReplaceDDLSuiteBase extends QueryTest
  with SharedSparkSession
  with ClusteredTableTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_UPDATE_CATALOG_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    // Reset UpdateCatalog's thread pool to ensure it is re-initialized in the next test suite.
    // This is necessary because the [[SparkThreadLocalForwardingThreadPoolExecutor]]
    // retains a reference to the SparkContext. Without resetting, the new test suite would
    // reuse the same SparkContext from the previous suite, despite it being stopped.
    //
    // This will force the UpdateCatalog's background thread to use the new SparkContext.
    //
    // scalastyle:off line.size.limit
    // This is to avoid the following exception thrown from the UpdateCatalog's background thread:
    //  java.lang.IllegalStateException: Cannot call methods on a stopped SparkContext.
    //  This stopped SparkContext was created at:
    //
    //  org.apache.spark.sql.delta.skipping.clustering.ClusteredTableDDLDataSourceV2NameColumnMappingSuite.beforeAll
    //
    //  The currently active SparkContext was created at:
    //
    //  org.apache.spark.sql.delta.skipping.clustering.ClusteredTableDDLDataSourceV2Suite.beforeAll
    // scalastyle:on line.size.limit
    UpdateCatalog.tp = null

    super.afterAll()
  }

  protected val testTable: String = "test_ddl_table"
  protected val sourceTable: String = "test_ddl_source"
  protected val targetTable: String = "test_ddl_target"

  protected def isPathBased: Boolean = false

  protected def supportedClauses: Seq[String]

  testCtasRtasHelper(supportedClauses)
  testClusteringColumnsPartOfStatsColumn(supportedClauses)
  testColTypeValidation("CREATE")

  def testCtasRtasHelper(clauses: Seq[String]): Unit = {
    Seq(
      ("",
        "a INT, b STRING, ts TIMESTAMP",
        Seq("a", "b")),
      (" multipart name",
        "a STRUCT<b INT, c STRING>, ts TIMESTAMP",
        Seq("a.b", "ts"))
    ).foreach { case (testSuffix, columns, clusteringColumns) =>
      test(s"create/replace table$testSuffix") {
        withTable(testTable) {
          clauses.foreach { clause =>
            createOrReplaceClusteredTable(
              clause, testTable, columns, clusteringColumns.mkString(","))
            verifyClusteringColumns(TableIdentifier(testTable), clusteringColumns)
          }
        }
      }

      test(s"ctas/rtas$testSuffix") {
        withTable(sourceTable, targetTable) {
          sql(s"CREATE TABLE $sourceTable($columns) USING delta")
          withTempDirIfNecessary { location =>
            clauses.foreach { clause =>
              createOrReplaceAsSelectClusteredTable(
                clause,
                targetTable,
                sourceTable,
                clusteringColumns.mkString(","),
                location = location)
              verifyClusteringColumns(targetTable, clusteringColumns, location)
            }
          }
        }
      }

      if (clauses.contains("REPLACE")) {
        test(s"Replace from non clustered table$testSuffix") {
          withTable(targetTable) {
            sql(s"CREATE TABLE $targetTable($columns) USING delta")
            createOrReplaceClusteredTable(
              "REPLACE", targetTable, columns, clusteringColumns.mkString(","))
            verifyClusteringColumns(TableIdentifier(targetTable), clusteringColumns)
          }
        }
      }
    }
  }

  protected def createTableWithStatsColumns(
      clause: String,
      table: String,
      clusterColumns: Seq[String],
      numIndexedColumns: Int,
      tableSchema: Option[String],
      statsColumns: Seq[String] = Seq.empty,
      location: Option[String] = None): Unit = {
    val clusterSpec = clusterColumns.mkString(",")
    val updatedTableProperties =
      collection.mutable.Map("delta.dataSkippingNumIndexedCols" -> s"$numIndexedColumns")
    if (statsColumns.nonEmpty) {
      updatedTableProperties(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key) =
        statsColumns.mkString(",")
    }
    val tablePropertiesString = updatedTableProperties.map {
      case (key, value) => s"'$key' = '$value'"
    }.mkString(",")
    val locationClause = if (location.isEmpty) "" else s"LOCATION '${location.get}'"
    if (clause == "REPLACE") {
      // Create the default before it can be replaced.
      sql(s"CREATE TABLE IF NOT EXISTS $table USING DELTA $locationClause")
    }
    if (tableSchema.isEmpty) {
      sql(
        s"""
           |$clause TABLE $table USING DELTA CLUSTER BY ($clusterSpec)
           |TBLPROPERTIES($tablePropertiesString)
           |$locationClause
           |AS SELECT * FROM $sourceTable
           |""".stripMargin)
    } else {
      createOrReplaceClusteredTable(
        clause, table, tableSchema.get, clusterSpec, updatedTableProperties.toMap, location)
    }
  }

  protected def testStatsCollectionHelper(
      tableSchema: String,
      numberOfIndexedCols: Int)(cb: => Unit): Unit = {
    withTable(sourceTable) {
      // Create a source table for CTAS.
      sql(
        s"""
           | CREATE TABLE $sourceTable($tableSchema) USING DELTA
           | TBLPROPERTIES('delta.dataSkippingNumIndexedCols' = '$numberOfIndexedCols')
           |""".stripMargin)
      // Run additional steps.
      cb
    }
  }

  protected def testColTypeValidation(clause: String): Unit = {
    test(s"validate column datatype checking on $clause table") {
      withTable("srcTbl", "dstTbl") {
        // Create reference table for CTAS/RTAS.
        sql(s"CREATE table srcTbl (a STRUCT<b INT, c INT>, d BOOLEAN, e MAP<INT, INT>) USING delta")

        val data = (0 to 1000).map(i => Row(Row(i + 1, i * 10), i % 2 == 0, Map(i -> i)))
        val schema = StructType(List(
          StructField("a", StructType(
            Array(
              StructField("b", IntegerType),
              StructField("c", IntegerType)
            )
          ))))
        spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))
          .write.mode("append").format("delta").saveAsTable("srcTbl")

        val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, new TableIdentifier("srcTbl"))
        // Test multiple data types.
        Seq("a", "d", "e").foreach { colName =>
          withTempDir { tmpDir =>
            // Since validation happens both on create and replace, validate for both cases to
            // ensure that datatype validation behaves consistently between the two.
            if (clause == "REPLACE") {
              sql("DROP TABLE IF EXISTS dstTbl")
              sql(s"CREATE TABLE dstTbl LIKE srcTbl LOCATION '${tmpDir.getAbsolutePath}'")
            }

            Seq(
              // Scenario 1: Standard CREATE/REPLACE TABLE.
              () => {
                val schema = "a STRUCT<b INT, c INT>, d BOOLEAN, e MAP<INT, INT>"
                createOrReplaceClusteredTable(
                  clause, "dstTbl", schema, colName, location = Some(tmpDir.getAbsolutePath))
              },
              // Scenario 2: CTAS/RTAS.
              () =>
                createOrReplaceAsSelectClusteredTable(
                clause, "dstTbl", "srcTbl", colName, location = Some(tmpDir.getAbsolutePath)))
              .foreach { f =>
                val e = intercept[DeltaAnalysisException] {
                  f()
                }
                checkError(
                  exception = e,
                  errorClass = "DELTA_CLUSTERING_COLUMN_MISSING_STATS",
                  parameters = Map(
                    "columns" -> colName,
                    "schema" -> """root
                      | |-- a: struct (nullable = true)
                      | |    |-- b: integer (nullable = true)
                      | |    |-- c: integer (nullable = true)
                      | |-- d: boolean (nullable = true)
                      | |-- e: map (nullable = true)
                      | |    |-- key: integer
                      | |    |-- value: integer (valueContainsNull = true)
                      |""".stripMargin)
                )
              }
          }
        }
      }
    }
  }

  test("cluster by with more than 4 columns - create table") {
    val testTable = "test_table"
    withTable(testTable) {
      val e = intercept[DeltaAnalysisException] {
        createOrReplaceClusteredTable(
          "CREATE", testTable, "a INT, b INT, c INT, d INT, e INT", "a, b, c, d, e")
      }
      checkError(
        exception = e,
        errorClass = "DELTA_CLUSTER_BY_INVALID_NUM_COLUMNS",
        parameters = Map("numColumnsLimit" -> "4", "actualNumColumns" -> "5")
      )
    }
  }

  test("cluster by with more than 4 columns - ctas") {
    val testTable = "test_table"
    val schema = "a INT, b INT, c INT, d INT, e INT"
    withTempDirIfNecessary { location =>
      withTable(sourceTable, testTable) {
        sql(s"CREATE TABLE $sourceTable($schema) USING delta")
        val e = intercept[DeltaAnalysisException] {
          createOrReplaceAsSelectClusteredTable(
            "CREATE", testTable, sourceTable, "a, b, c, d, e", location = location)
        }
        checkError(
          exception = e,
          errorClass = "DELTA_CLUSTER_BY_INVALID_NUM_COLUMNS",
          parameters = Map("numColumnsLimit" -> "4", "actualNumColumns" -> "5")
        )
      }
    }
  }

  protected def verifyPartitionColumns(
      tableIdentifier: TableIdentifier,
      expectedPartitionColumns: Seq[String]): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)
    assert(snapshot.metadata.partitionColumns === expectedPartitionColumns)
  }

  protected def verifyClusteringColumns(
      table: String,
      expectedLogicalClusteringColumns: Seq[String],
      locationOpt: Option[String]): Unit = {
    locationOpt.map { location =>
      verifyClusteringColumns(
        location, expectedLogicalClusteringColumns
      )
    }.getOrElse {
      verifyClusteringColumns(TableIdentifier(table), expectedLogicalClusteringColumns)
    }
  }

  def testClusteringColumnsPartOfStatsColumn(clauses: Seq[String]): Unit = {
    clauses.foreach { clause =>
      val mode = if (clause == "CREATE") "create table" else "replace table"
      test(s"Validate clustering columns part of stats columns - $mode") {
        val tableSchema = "col0 int, col1 STRUCT<col11: int, col12: string>, col2 int"
        val indexedColumns = 2
        testStatsCollectionHelper(
          tableSchema = tableSchema,
          numberOfIndexedCols = indexedColumns) {
          withTable(targetTable) {
            val deltaLogSrc = DeltaLog.forTable(spark, TableIdentifier(sourceTable))
            // Validate the 3rd column `col1.col12` and 4th column `col2` can not be
            // clustering columns.
            val e = intercept[DeltaAnalysisException](
              createTableWithStatsColumns(
                clause,
                targetTable,
                "col0" :: "col1.col11" :: "col1.col12" :: "col2" :: Nil,
                indexedColumns,
                Some(tableSchema)))
            checkError(
              exception = e,
              errorClass = "DELTA_CLUSTERING_COLUMN_MISSING_STATS",
              parameters = Map(
                "columns" -> "col1.col12, col2",
                "schema" -> """root
                    | |-- col0: integer (nullable = true)
                    | |-- col1: struct (nullable = true)
                    | |    |-- col11: integer (nullable = true)
                    |""".stripMargin)
            )
            // Validate the first two columns can be clustering columns.
            createTableWithStatsColumns(
              clause,
              targetTable,
              "col0" :: "col1.col11" :: Nil,
              indexedColumns,
              Some(tableSchema))
          }
        }
      }
    }

    clauses.foreach { clause =>
      val mode = if (clause == "CREATE") "ctas" else "rtas"
      test(s"Validate clustering columns part of stats columns - $mode") {
        // Add a suffix for the target table name to work around the issue that delta table's
        // location isn't removed by the DROP TABLE from ctas/rtas test cases.
        val table = targetTable + "_" + clause

        val tableSchema = "col0 int, col1 STRUCT<col11: int, col12: string>, col2 int"
        val indexedColumns = 2
        testStatsCollectionHelper(
          tableSchema = tableSchema,
          numberOfIndexedCols = indexedColumns) {
          withTable(table) {
            withTempDir { dir =>
              val deltaLogSrc = DeltaLog.forTable(spark, TableIdentifier(sourceTable))
              val targetLog = DeltaLog.forTable(spark, s"${dir.getPath}")
              val dataPath = new File(targetLog.dataPath.toString.replace("file:", ""))
              val initialNumFiles =
                if (dataPath.listFiles() != null) { // Returns null if directory doesn't exist -> 0
                  dataPath.listFiles().size
                }
                else {
                  0
                }
              // Validate the 3rd column `col1.col12` and 4th column `col2` can not be
              // clustering columns.
              val e = intercept[DeltaAnalysisException](
                createTableWithStatsColumns(
                  clause,
                  table,
                  "col0" :: "col1.col11" :: "col1.col12" :: "col2" :: Nil,
                  indexedColumns,
                  None,
                  location = Some(dir.getPath)))
              checkError(
                exception = e,
                errorClass = "DELTA_CLUSTERING_COLUMN_MISSING_STATS",
                parameters = Map(
                  "columns" -> "col1.col12, col2",
                  "schema" -> """root
                    | |-- col0: integer (nullable = true)
                    | |-- col1: struct (nullable = true)
                    | |    |-- col11: integer (nullable = true)
                    |""".stripMargin)
              )

              // Validate the first two columns can be clustering columns.
              createTableWithStatsColumns(
                clause,
                table,
                "col0" :: "col1.col11" :: Nil,
                indexedColumns,
                None)
            }
          }
        }
      }
    }
  }

  test("Validate clustering columns cannot be non-eligible data types") {
    val indexedColumns = 3
    // Validate non-eligible column stat data type.
    val nonEligibleType = ArrayType(IntegerType)
    assert(!SkippingEligibleDataType(nonEligibleType))
    val nonEligibleTableSchema = s"col0 int, col1 STRUCT<col11: array<int>, col12: string>"
    testStatsCollectionHelper(
      tableSchema = nonEligibleTableSchema,
      numberOfIndexedCols = indexedColumns) {
      withTable(targetTable) {
        val deltaLogSrc = DeltaLog.forTable(spark, TableIdentifier(sourceTable))
        // Validate the 2nd column `col1.col11` cannot be clustering column.
        val e = intercept[DeltaAnalysisException](
          createTableWithStatsColumns(
            "CREATE",
            targetTable,
            "col0" :: "col1.col11" :: Nil,
            indexedColumns,
            Some(nonEligibleTableSchema)))
        checkError(
          exception = e,
          errorClass = "DELTA_CLUSTERING_COLUMN_MISSING_STATS",
          parameters = Map(
            "columns" -> "col1.col11",
            "schema" -> """root
              | |-- col0: integer (nullable = true)
              | |-- col1: struct (nullable = true)
              | |    |-- col11: array (nullable = true)
              | |    |    |-- element: integer (containsNull = true)
              | |    |-- col12: string (nullable = true)
              |""".stripMargin)
        )
      }
    }
  }

  test("Replace clustered table with non-clustered table") {
    import testImplicits._
    withTable(sourceTable) {
      sql(s"CREATE TABLE $sourceTable(i int, s string) USING delta")
      spark.range(1000)
        .map(i => (i.intValue(), "string col"))
        .toDF("i", "s")
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(sourceTable)

      // Validate REPLACE TABLE (AS SELECT).
      Seq("REPLACE", "CREATE OR REPLACE").foreach { clause =>
        withClusteredTable(testTable, "a int", "a") {
          verifyClusteringColumns(TableIdentifier(testTable), Seq("a"))

          Seq(true, false).foreach { isRTAS =>
            val testQuery = if (isRTAS) {
              s"$clause TABLE $testTable USING delta AS SELECT * FROM $sourceTable"
            } else {
              sql(s"$clause TABLE $testTable (i int, s string) USING delta")
              s"INSERT INTO $testTable SELECT * FROM $sourceTable"
            }
            sql(testQuery)
            // Note that clustering table feature are still retained after REPLACE TABLE.
            verifyClusteringColumns(TableIdentifier(testTable), Seq.empty)
          }
        }
      }
    }
  }

  test("Replace clustered table with non-clustered table - dataframe writer") {
    import testImplicits._
    withTable(sourceTable) {
      sql(s"CREATE TABLE $sourceTable(i int, s string) USING delta")
      spark.range(1000)
        .map(i => (i.intValue(), "string col"))
        .toDF("i", "s")
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(sourceTable)

      withClusteredTable(testTable, "a int", "a") {
        verifyClusteringColumns(TableIdentifier(testTable), Seq("a"))

        spark.table(sourceTable)
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(testTable)
        // Note that clustering table feature are still retained after REPLACE TABLE.
        verifyClusteringColumns(TableIdentifier(testTable), Seq.empty)
      }
    }
  }

  protected def withTempDirIfNecessary(f: Option[String] => Unit): Unit = {
    if (isPathBased) {
      withTempDir { dir =>
        f(Some(dir.getAbsolutePath))
      }
    } else {
      f(None)
    }
  }
}

trait ClusteredTableDDLWithColumnMapping
  extends ClusteredTableCreateOrReplaceDDLSuite
    with DeltaColumnMappingSelectedTestMixin {

  override protected def runOnlyTests: Seq[String] = Seq(
    "validate dropping clustering column is not allowed: single clustering column",
    "validate dropping clustering column is not allowed: multiple clustering columns",
    "validate dropping clustering column is not allowed: clustering column + " +
      "non-clustering column",
    "validate RESTORE on clustered table"
  )

  test("validate dropping clustering column is not allowed: single clustering column") {
    withClusteredTable(testTable, "col1 INT, col2 STRING, col3 LONG", "col1") {
      val e = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE $testTable DROP COLUMNS (col1)")
      }
      checkError(
        exception = e,
        errorClass = "DELTA_UNSUPPORTED_DROP_CLUSTERING_COLUMN",
        parameters = Map("columnList" -> "col1")
      )
      // Drop non-clustering columns are allowed.
      sql(s"ALTER TABLE $testTable DROP COLUMNS (col2)")
    }
  }

  test("validate dropping clustering column is not allowed: multiple clustering columns") {
    withClusteredTable(testTable, "col1 INT, col2 STRING, col3 LONG", "col1, col2") {
      val e = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE $testTable DROP COLUMNS (col1, col2)")
      }
      checkError(
        exception = e,
        errorClass = "DELTA_UNSUPPORTED_DROP_CLUSTERING_COLUMN",
        parameters = Map("columnList" -> "col1,col2")
      )
    }
  }

  test("validate dropping clustering column is not allowed: clustering column + " +
    "non-clustering column") {
    withClusteredTable(testTable, "col1 INT, col2 STRING, col3 LONG", "col1, col2") {
      val e = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE $testTable DROP COLUMNS (col1, col3)")
      }
      checkError(
        exception = e,
        errorClass = "DELTA_UNSUPPORTED_DROP_CLUSTERING_COLUMN",
        parameters = Map("columnList" -> "col1")
      )
    }
  }
}

trait ClusteredTableDDLWithColumnMappingV2Base extends ClusteredTableDDLWithColumnMapping {
  test("test clustering column names (alter table + create table) with spaces") {
    withClusteredTable(testTable, "`col1 a` INT, col2 INT, col3 STRUCT<col4 INT, `col5 b` INT>, " +
      "`col6 c` STRUCT<col7 INT, `col8 d.e` INT>, `col9.f` INT", "`col1 a`") {
      val tableIdentifier = TableIdentifier(testTable)
      verifyClusteringColumns(tableIdentifier, Seq("`col1 a`"))

      // Test ALTER CLUSTER BY to change clustering columns away from names with spaces.
      sql(s"ALTER TABLE $testTable CLUSTER BY (col2)")
      verifyClusteringColumns(tableIdentifier, Seq("col2"))

      // Test ALTER CLUSTER BY to test with structs with spaces in varying places.
      sql(s"ALTER TABLE $testTable CLUSTER BY (col3.`col5 b`, `col6 c`.col7)")
      verifyClusteringColumns(tableIdentifier, Seq("col3.`col5 b`", "`col6 c`.col7"))

      // Test ALTER CLUSTER BY on structs with spaces on both entries and with no spaces in the same
      // clustering spec, including cases where there is a '.' in the name.
      sql(s"ALTER TABLE $testTable CLUSTER BY (col3.col4, `col6 c`.`col8 d.e`, `col1 a`)")
      verifyClusteringColumns(tableIdentifier, Seq("col3.col4", "`col6 c`.`col8 d.e`", "`col1 a`"))

      // Test ALTER TABLE CLUSTER BY after renaming a column to include spaces in the name.
      sql(s"ALTER TABLE $testTable RENAME COLUMN col2 to `col2 e`")
      sql(s"ALTER TABLE $testTable CLUSTER BY (`col2 e`)")
      verifyClusteringColumns(tableIdentifier, Seq("`col2 e`"))

      // Test ALTER TABLE with '.' in the name.
      sql(s"ALTER TABLE $testTable CLUSTER BY (`col9.f`)")
      verifyClusteringColumns(tableIdentifier, Seq("`col9.f`"))
    }
  }

  test("validate create table with commas in the column name") {
    withClusteredTable(testTable, "`col1,a` BIGINT", "`col1,a`") {
      verifyClusteringColumns(TableIdentifier(testTable), Seq("`col1,a`"))
    }
    withClusteredTable(testTable, "`,col1,a,` BIGINT", "`,col1,a,`") {
      verifyClusteringColumns(TableIdentifier(testTable), Seq("`,col1,a,`"))
    }
    withClusteredTable(testTable, "`,col1,a,` BIGINT, `col2` BIGINT", "`,col1,a,`, `col2`") {
      verifyClusteringColumns(TableIdentifier(testTable), Seq("`,col1,a,`", "col2"))
    }
    withClusteredTable(testTable, "`,col1,a,` BIGINT, col2 BIGINT", "col2") {
      sql(s"ALTER TABLE $testTable CLUSTER BY (`,col1,a,`)")
      verifyClusteringColumns(TableIdentifier(testTable), Seq("`,col1,a,`"))
    }
  }
}

trait ClusteredTableDDLWithColumnMappingV2
  extends ClusteredTableDDLWithColumnMappingV2Base

trait ClusteredTableCreateOrReplaceDDLSuite
  extends ClusteredTableCreateOrReplaceDDLSuiteBase

trait ClusteredTableDDLSuiteBase
  extends ClusteredTableCreateOrReplaceDDLSuite
    with DeltaSQLCommandTest
    with DeltaExcludedBySparkVersionTestMixinShims {

  import testImplicits._

  test("cluster by with more than 4 columns - alter table") {
    val testTable = "test_table"
    withClusteredTable(testTable, "a INT, b INT, c INT, d INT, e INT", "a") {
      val e = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE $testTable CLUSTER BY (a, b, c, d, e)")
      }
      checkError(
        e,
        errorClass = "DELTA_CLUSTER_BY_INVALID_NUM_COLUMNS",
        parameters = Map(
          "numColumnsLimit" -> "4",
          "actualNumColumns" -> "5")
      )
    }
  }

  test("alter table cluster by - valid scenarios") {
    withClusteredTable(testTable, "id INT, a STRUCT<b INT, c STRING>, name STRING", "id, name") {
      val tableIdentifier = TableIdentifier(testTable)
      verifyClusteringColumns(tableIdentifier, Seq("id", "name"))

      // Change the clustering columns and verify that they are changed in both
      // Delta logs and catalog.
      sql(s"ALTER TABLE $testTable CLUSTER BY (name)")
      verifyClusteringColumns(tableIdentifier, Seq("name"))

      // Nested column scenario.
      sql(s"ALTER TABLE $testTable CLUSTER BY (a.b, id)")
      verifyClusteringColumns(tableIdentifier, Seq("a.b", "id"))
    }
  }

  test("alter table cluster by - catalog reflects clustering columns when reordered") {
    withClusteredTable(testTable, "id INT, a STRUCT<b INT, c STRING>, name STRING", "id, name") {
      val tableIdentifier = TableIdentifier(testTable)
      verifyClusteringColumns(tableIdentifier, Seq("id", "name"))

      // Re-order the clustering keys and validate the catalog sees the correctly reordered keys.
      sql(s"ALTER TABLE $testTable CLUSTER BY (name, id)")
      verifyClusteringColumns(tableIdentifier, Seq("name", "id"))
    }
  }

  test("alter table cluster by - error scenarios") {
    withClusteredTable(testTable, "id INT, id2 INT, name STRING", "id, name") {
      // Specify non-existing columns.
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $testTable CLUSTER BY (invalid)")
      }
      assert(e.getMessage.contains("Couldn't find column"))

      // Specify duplicate clustering columns.
      val e2 = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE $testTable CLUSTER BY (id, id)")
      }
      assert(e2.getErrorClass == "DELTA_DUPLICATE_COLUMNS_FOUND")
      assert(e2.getSqlState == "42711")
      assert(e2.getMessageParametersArray === Array("in CLUSTER BY", "`id`"))
    }
  }

  test("alter table cluster by none") {
    withClusteredTable(testTable, "id Int", "id") {
      val tableIdentifier = TableIdentifier(testTable)
      verifyClusteringColumns(tableIdentifier, Seq("id"))

      sql(s"ALTER TABLE $testTable CLUSTER BY NONE")
      verifyClusteringColumns(tableIdentifier, Seq.empty)
    }
  }

  test("optimize clustered table and trigger regular compaction") {
    withClusteredTable(testTable, "a INT, b STRING", "a, b") {
      val tableIdentifier = TableIdentifier(testTable)
      verifyClusteringColumns(tableIdentifier, Seq("a", "b"))

      (1 to 1000).map(i => (i, i.toString)).toDF("a", "b")
        .write.mode("append").format("delta").saveAsTable(testTable)

      val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTable))
      val targetFileSize = (snapshot.sizeInBytes / 10).toString
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> targetFileSize,
        DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE.key -> targetFileSize) {
        runOptimize(testTable) { metrics =>
          assert(metrics.numFilesAdded > 0)
          assert(metrics.numFilesRemoved > 0)
          assert(metrics.clusteringStats.nonEmpty)
          assert(metrics.clusteringStats.get.numOutputZCubes == 1)
        }
      }

      // ALTER TABLE CLUSTER BY NONE and then OPTIMIZE to trigger regular compaction.
      sql(s"ALTER TABLE $testTable CLUSTER BY NONE")
      verifyClusteringColumns(tableIdentifier, Seq.empty)

      (1001 to 2000).map(i => (i, i.toString)).toDF("a", "b")
        .repartition(10).write.mode("append").format("delta").saveAsTable(testTable)
      val newSnapshot = deltaLog.update()
      val newTargetFileSize = (newSnapshot.sizeInBytes / 10).toString
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> newTargetFileSize,
        DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE.key -> newTargetFileSize) {
        runOptimize(testTable) { metrics =>
          assert(metrics.numFilesAdded > 0)
          assert(metrics.numFilesRemoved > 0)
          // No clustering or zorder stats indicates regular compaction.
          assert(metrics.clusteringStats.isEmpty)
          assert(metrics.zOrderStats.isEmpty)
        }
      }
    }
  }

  test("optimize clustered table - error scenarios") {
    withClusteredTable(testTable, "a INT, b STRING", "a") {
      // Specify partition predicate.
      val e = intercept[DeltaUnsupportedOperationException] {
        sql(s"OPTIMIZE $testTable WHERE a > 0 and b = foo")
      }
      checkError(
        e,
        "DELTA_CLUSTERING_WITH_PARTITION_PREDICATE",
        parameters = Map("predicates" -> "a > 0 and b = foo")
      )

      // Specify ZORDER BY.
      val e2 = intercept[DeltaAnalysisException] {
        sql(s"OPTIMIZE $testTable ZORDER BY (a)")
      }
      checkError(
        exception = e2,
        errorClass = "DELTA_CLUSTERING_WITH_ZORDER_BY",
        parameters = Map("zOrderBy" -> "a")
      )
    }
  }

  test("Validate stats collected - alter table") {
    val tableSchema = "col0 int, col1 STRUCT<col11: int, col12: string>"
    val indexedColumns = 2
    // Validate ALTER TABLE can not change to a missing stats column.
    testStatsCollectionHelper(
      tableSchema = tableSchema,
      numberOfIndexedCols = indexedColumns) {
      withTable(testTable) {
        createTableWithStatsColumns(
          "CREATE",
          testTable,
          "col0" :: "col1.col11" :: Nil,
          indexedColumns,
          Some(tableSchema))
        // Try to alter to col1.col12 which is missing stats.
        val e = intercept[DeltaAnalysisException] {
          sql(
            s"""
               |ALTER TABLE $testTable
               |CLUSTER BY (col0, col1.col12)
               |""".stripMargin)
        }
        val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTable))
        checkError(
          e,
          "DELTA_CLUSTERING_COLUMN_MISSING_STATS",
          parameters = Map(
            "columns" -> "col1.col12",
            "schema" -> snapshot.statCollectionLogicalSchema.treeString)
        )
      }
    }
  }

  test("validate CLONE on clustered table") {
    import testImplicits._
    val srcTable = "SrcTbl"
    val dstTable1 = "DestTbl1"
    val dstTable2 = "DestTbl2"
    val dstTable3 = "DestTbl3"

    withTable(srcTable, dstTable1, dstTable2, dstTable3) {
      // Create the source table.
      sql(s"CREATE TABLE $srcTable (col1 INT, col2 INT, col3 INT) " +
        s"USING delta CLUSTER BY (col1, col2)")
      val tableIdent = new TableIdentifier(srcTable)
      (1 to 100).map(i => (i, i + 1000, i + 100)).toDF("col1", "col2", "col3")
        .repartitionByRange(100, col("col1"))
        .write.format("delta").mode("append").saveAsTable(srcTable)

      // Force clustering on the source table.
      val (_, srcSnapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdent)
      val ingestionSize = srcSnapshot.allFiles.collect().map(_.size).sum
      withSQLConf(
        DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> (ingestionSize / 4).toString) {
        runOptimize(srcTable) { res =>
          assert(res.numFilesAdded === 4)
          assert(res.numFilesRemoved === 100)
        }
      }

      // Create destination table as a clone of the source table.
      sql(s"CREATE TABLE $dstTable1 SHALLOW CLONE $srcTable")

      // Validate clustering columns and that clustering columns in stats schema.
      val (_, dstSnapshot1) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(dstTable1))
      verifyClusteringColumns(TableIdentifier(dstTable1), Seq("col1", "col2"))
      ClusteredTableUtils.validateClusteringColumnsInStatsSchema(dstSnapshot1, Seq("col1", "col2"))

      // Change to CLUSTER BY NONE, then CLONE from earlier version to validate that the
      // clustering column information is maintainted.
      sql(s"ALTER TABLE $srcTable CLUSTER BY NONE")
      sql(s"CREATE TABLE $dstTable2 SHALLOW CLONE $srcTable VERSION AS OF 2")
      val (_, dstSnapshot2) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(dstTable2))
      verifyClusteringColumns(TableIdentifier(dstTable2), Seq("col1", "col2"))
      ClusteredTableUtils.validateClusteringColumnsInStatsSchema(dstSnapshot2, Seq("col1", "col2"))

      // Validate CLONE after CLUSTER BY NONE
      sql(s"CREATE TABLE $dstTable3 SHALLOW CLONE $srcTable")
      val (_, dstSnapshot3) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(dstTable3))
      verifyClusteringColumns(TableIdentifier(dstTable3), Seq.empty)
      ClusteredTableUtils.validateClusteringColumnsInStatsSchema(dstSnapshot3, Seq.empty)

    }
  }

  test("alter table cluster by none is a no-op on non-clustered tables") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (a INT, b STRING) USING delta")
      val tableIdentifier = TableIdentifier(testTable)
      val (_, initialSnapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)

      // Verify that ALTER TABLE CLUSTER BY NONE does not enable clustering and is a no-op.
      val clusterByLogs = Log4jUsageLogger.track {
        sql(s"ALTER TABLE $testTable CLUSTER BY NONE")
      }.filter { e =>
        e.metric == MetricDefinitions.EVENT_TAHOE.name &&
          e.tags.get("opType").contains("delta.ddl.alter.clusterBy")
      }
      assert(clusterByLogs.nonEmpty)
      val clusterByLogJson = JsonUtils.fromJson[Map[String, Any]](clusterByLogs.head.blob)
      assert(clusterByLogJson("isClusterByNoneSkipped").asInstanceOf[Boolean])
      val (_, finalSnapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)
      assert(!ClusteredTableUtils.isSupported(finalSnapshot.protocol))

      // Snapshot equality shows that no table features can be changed.
      assert(initialSnapshot.version == finalSnapshot.version)
      assert(initialSnapshot.protocol.readerAndWriterFeatureNames ==
        finalSnapshot.protocol.readerAndWriterFeatureNames)
    }
  }

  test("alter table set tbl properties not allowed for clusteringColumns") {
    withClusteredTable(testTable, "a INT, b STRING", "a") {
      val e = intercept[DeltaUnsupportedOperationException] {
        sql(s"""
           |ALTER TABLE $testTable SET TBLPROPERTIES
           |('${ClusteredTableUtils.PROP_CLUSTERING_COLUMNS}' = '[[\"b\"]]')
           |""".stripMargin)
      }
      checkError(
        e,
        errorClass = "DELTA_CANNOT_MODIFY_TABLE_PROPERTY",
        parameters = Map("prop" -> "clusteringColumns"))
    }
  }

  test("validate RESTORE on clustered table") {
    val tableIdentifier = TableIdentifier(testTable)
    // Scenario 1: restore clustered table to unclustered version.
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (a INT, b STRING) USING delta")
      val (_, startingSnapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)
      assert(!ClusteredTableUtils.isSupported(startingSnapshot.protocol))

      sql(s"ALTER TABLE $testTable CLUSTER BY (a)")
      verifyClusteringColumns(tableIdentifier, Seq("a"))

      sql(s"RESTORE TABLE $testTable TO VERSION AS OF 0")
      val (_, currentSnapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)
      verifyClusteringColumns(tableIdentifier, Seq.empty, skipCatalogCheck = true)
    }

    // Scenario 2: restore clustered table to previous clustering columns.
    withClusteredTable(testTable, "a INT, b STRING", "a") {
      verifyClusteringColumns(tableIdentifier, Seq("a"))

      sql(s"ALTER TABLE $testTable CLUSTER BY (b)")
      verifyClusteringColumns(tableIdentifier, Seq("b"))

      sql(s"RESTORE TABLE $testTable TO VERSION AS OF 0")
      verifyClusteringColumns(tableIdentifier, Seq("a"), skipCatalogCheck = true)
    }

    // Scenario 3: restore from table with clustering columns to non-empty clustering columns
    withClusteredTable(testTable, "a int", "a") {
      verifyClusteringColumns(tableIdentifier, Seq("a"))

      sql(s"ALTER TABLE $testTable CLUSTER BY NONE")
      verifyClusteringColumns(tableIdentifier, Seq.empty)

      sql(s"RESTORE TABLE $testTable TO VERSION AS OF 0")
      verifyClusteringColumns(tableIdentifier, Seq("a"), skipCatalogCheck = true)
    }

    // Scenario 4: restore to start version.
    withClusteredTable(testTable, "a int", "a") {
      verifyClusteringColumns(tableIdentifier, Seq("a"))

      sql(s"INSERT INTO $testTable VALUES (1)")

      sql(s"RESTORE TABLE $testTable TO VERSION AS OF 0")
      verifyClusteringColumns(tableIdentifier, Seq("a"), skipCatalogCheck = true)
    }

    // Scenario 5: restore unclustered table to unclustered table.
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (a INT) USING delta")
      val (_, startingSnapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)
      assert(!ClusteredTableUtils.isSupported(startingSnapshot.protocol))
      assert(!startingSnapshot.domainMetadata.exists(_.domain ==
        ClusteringMetadataDomain.domainName))

      sql(s"INSERT INTO $testTable VALUES (1)")

      sql(s"RESTORE TABLE $testTable TO VERSION AS OF 0").collect
      val (_, currentSnapshot) = DeltaLog.forTableWithSnapshot(spark, tableIdentifier)
      assert(!ClusteredTableUtils.isSupported(currentSnapshot.protocol))
      assert(!currentSnapshot.domainMetadata.exists(_.domain ==
        ClusteringMetadataDomain.domainName))
    }
  }

  testSparkMasterOnly("Variant is not supported") {
    val e = intercept[DeltaAnalysisException] {
      createOrReplaceClusteredTable("CREATE", testTable, "id long, v variant", "v")
    }
    checkError(
      e,
      "DELTA_CLUSTERING_COLUMN_MISSING_STATS",
      parameters = Map(
        "columns" -> "v",
        "schema" -> """root
                      | |-- id: long (nullable = true)
                      | |-- v: variant (nullable = true)
                      |""".stripMargin)
    )
  }
}

trait ClusteredTableDDLSuite extends ClusteredTableDDLSuiteBase

trait ClusteredTableDDLWithNameColumnMapping
  extends ClusteredTableCreateOrReplaceDDLSuite with DeltaColumnMappingEnableNameMode

trait ClusteredTableDDLWithIdColumnMapping
  extends ClusteredTableCreateOrReplaceDDLSuite with DeltaColumnMappingEnableIdMode

trait ClusteredTableDDLWithV2Base
  extends ClusteredTableCreateOrReplaceDDLSuite
    with SharedSparkSession {
  override protected def supportedClauses: Seq[String] = Seq("CREATE", "REPLACE")

  testColTypeValidation("REPLACE")

  test("replace with different clustering columns") {
    withTable(sourceTable) {
      sql(s"CREATE TABLE $sourceTable(i int, s string) USING delta")
      // Validate REPLACE TABLE (AS SELECT).
      Seq("REPLACE", "CREATE OR REPLACE").foreach { clause =>
        Seq(true, false).foreach { isRTAS =>
          withTempDirIfNecessary { location =>
            withClusteredTable(testTable, "a int", "a", location = location) {
              if (isRTAS) {
                createOrReplaceAsSelectClusteredTable(
                  clause, testTable, sourceTable, "i", location = location)
              } else {
                createOrReplaceClusteredTable(
                  clause, testTable, "i int, b string", "i", location = location)
              }
              verifyClusteringColumns(testTable, Seq("i"), location)
            }
          }
        }
      }
    }
  }

  test("Validate replacing clustered tables with partitioned tables is not allowed") {
    withTable(sourceTable) {
      sql(s"CREATE TABLE $sourceTable(i int, s string) USING delta")

      // Validate REPLACE TABLE (AS SELECT).
      Seq("REPLACE", "CREATE OR REPLACE").foreach { clause =>
        withClusteredTable(testTable, "a int", "a") {
          verifyClusteringColumns(TableIdentifier(testTable), Seq("a"))

          Seq(true, false).foreach { isRTAS =>
            val e = intercept[DeltaAnalysisException] {
              if (isRTAS) {
                sql(s"$clause TABLE $testTable USING delta PARTITIONED BY (i) " +
                  s"AS SELECT * FROM $sourceTable")
              } else {
                sql(s"$clause TABLE $testTable (i int, b string) USING delta PARTITIONED BY (i)")
              }
            }
            checkError(
              e,
              "DELTA_CLUSTERING_REPLACE_TABLE_WITH_PARTITIONED_TABLE",
              parameters = Map.empty
            )
          }
        }
      }
    }
  }

  test("Validate replacing partitioned tables with clustered tables is allowed") {
    withTable(sourceTable) {
      sql(s"CREATE TABLE $sourceTable(i int, s string) USING delta")

      // Validate REPLACE TABLE (AS SELECT).
      Seq("REPLACE", "CREATE OR REPLACE").foreach { clause =>
        Seq(true, false).foreach { isRTAS =>
          withTable(testTable) {
            withTempDirIfNecessary { location =>
              val locationClause = if (location.isEmpty) "" else s"LOCATION '${location.get}'"
              sql(s"CREATE TABLE $testTable USING delta PARTITIONED BY (i) $locationClause" +
                s" SELECT 1 i, 'a' s")
              verifyPartitionColumns(TableIdentifier(testTable), Seq("i"))
              if (isRTAS) {
                createOrReplaceAsSelectClusteredTable(
                  clause, testTable, sourceTable, "i", location = location)
              } else {
                createOrReplaceClusteredTable(
                  clause, testTable, "i int, b string", "i", location = location)
              }
              verifyClusteringColumns(testTable, Seq("i"), location)
              verifyPartitionColumns(TableIdentifier(testTable), Seq())
            }
          }
        }
      }
    }
  }

  Seq(
    ("",
      "a INT, b STRING, ts TIMESTAMP",
      Seq("a", "b")),
    (" multipart name",
      "a STRUCT<b INT, c STRING>, ts TIMESTAMP",
      Seq("a.b", "ts"))
  ).foreach { case (testSuffix, columns, clusteringColumns) =>
    test(s"create/replace table createOrReplace$testSuffix") {
      withTable(testTable) {
        // Repeat two times to test both create and replace cases.
        (1 to 2).foreach { _ =>
          createOrReplaceClusteredTable(
            "CREATE OR REPLACE", testTable, columns, clusteringColumns.mkString(","))
          verifyClusteringColumns(TableIdentifier(testTable), clusteringColumns)
        }
      }
    }

    test(s"ctas/rtas createOrReplace$testSuffix") {
      withTable(sourceTable, targetTable) {
        sql(s"CREATE TABLE $sourceTable($columns) USING delta")
        withTempDirIfNecessary { location =>
          // Repeat two times to test both create and replace cases.
          (1 to 2).foreach { _ =>
            createOrReplaceAsSelectClusteredTable(
              "CREATE OR REPLACE",
              targetTable,
              sourceTable,
              clusteringColumns.mkString(","),
              location = location)
            verifyClusteringColumns(targetTable, clusteringColumns, location)
          }
        }
      }
    }
  }
}

trait ClusteredTableDDLWithV2
  extends ClusteredTableDDLWithV2Base

trait ClusteredTableDDLDataSourceV2SuiteBase
  extends ClusteredTableDDLWithV2
    with ClusteredTableDDLSuite {
  test("Create clustered table from external location, " +
    "location has clustered table, schema not specified, cluster by not specified") {
    withTempDir { dir =>
      // 1. Create a clustered table
      sql(s"create table delta.`${dir.getAbsolutePath}` (col1 int, col2 string) using delta " +
        "cluster by (col1)")

      // 2. Create a clustered table from the external location.
      withTable("clustered_table") {
        // When schema is not specified, the schema of the table is inferred from the external
        // table.
        sql(s"CREATE EXTERNAL TABLE clustered_table USING delta LOCATION '${dir.getAbsolutePath}'")
        verifyClusteringColumns(TableIdentifier("clustered_table"), Seq("col1"))
      }
    }
  }

  test("create external non-clustered table: location has clustered table, schema specified, " +
    "cluster by not specified") {
    val tableName = "clustered_table"
    withTempDir { dir =>
      // 1. Create a clustered table in the external location.
      sql(s"create table delta.`${dir.getAbsolutePath}` (col1 int, col2 string) using delta " +
        "cluster by (col1)")

      // 2. Create a non-clustered table from the external location.
      withTable(tableName) {
        val e = intercept[DeltaAnalysisException] {
          // When schema is specified, the schema has to match the schema of the external table.
          sql(s"CREATE EXTERNAL TABLE $tableName (col1 INT, col2 STRING) USING delta " +
            s"LOCATION '${dir.getAbsolutePath}'")
        }
        checkError(
          e,
          errorClass = "DELTA_CREATE_TABLE_WITH_DIFFERENT_CLUSTERING",
          parameters = Map(
            "path" -> dir.toURI.toString.stripSuffix("/"),
            "specifiedColumns" -> "",
            "existingColumns" -> "col1"))
      }
    }
  }

  test("create external clustered table: location has clustered table, schema specified, " +
    "cluster by specified with different clustering column") {
    val tableName = "clustered_table"
    withTempDir { dir =>
      // 1. Create a clustered table in the external location.
      sql(s"create table delta.`${dir.getAbsolutePath}` (col1 int, col2 string) using delta " +
        "cluster by (col1)")

      // 2. Create a clustered table from the external location.
      withTable(tableName) {
        val e = intercept[DeltaAnalysisException] {
          sql(s"CREATE EXTERNAL TABLE $tableName (col1 INT, col2 STRING) USING delta " +
            s"CLUSTER BY (col2) LOCATION '${dir.getAbsolutePath}'")
        }
        checkError(
          e,
          errorClass = "DELTA_CREATE_TABLE_WITH_DIFFERENT_CLUSTERING",
          parameters = Map(
            "path" -> dir.toURI.toString.stripSuffix("/"),
            "specifiedColumns" -> "col2",
            "existingColumns" -> "col1"))
      }
    }
  }

  test("create external clustered table: location has clustered table, schema specified, " +
    "cluster by specified with same clustering column") {
    val tableName = "clustered_table"
    withTempDir { dir =>
      // 1. Create a clustered table in the external location.
      sql(s"create table delta.`${dir.getAbsolutePath}` (col1 int, col2 string) using delta " +
        "cluster by (col1)")

      // 2. Create a clustered table from the external location.
      withTable(tableName) {
        sql(s"CREATE EXTERNAL TABLE $tableName (col1 INT, col2 STRING) USING delta " +
          s"CLUSTER BY (col1) LOCATION '${dir.getAbsolutePath}'")
        verifyClusteringColumns(TableIdentifier(tableName), Seq("col1"))
      }
    }
  }

  test("create external clustered table: location has non-clustered table, schema specified, " +
    "cluster by specified") {
    val tableName = "clustered_table"
    withTempDir { dir =>
      // 1. Create a non-clustered table in the external location.
      sql(s"create table delta.`${dir.getAbsolutePath}` (col1 int, col2 string) using delta")

      // 2. Create a clustered table from the external location.
      withTable(tableName) {
        val e = intercept[DeltaAnalysisException] {
          sql(s"CREATE EXTERNAL TABLE $tableName (col1 INT, col2 STRING) USING delta " +
            s"CLUSTER BY (col1) LOCATION '${dir.getAbsolutePath}'")
        }
        checkError(
          e,
          errorClass = "DELTA_CREATE_TABLE_WITH_DIFFERENT_CLUSTERING",
          parameters = Map(
            "path" -> dir.toURI.toString.stripSuffix("/"),
            "specifiedColumns" -> "col1",
            "existingColumns" -> ""))
      }
    }
  }
}

class ClusteredTableDDLDataSourceV2Suite
  extends ClusteredTableDDLDataSourceV2SuiteBase

class ClusteredTableDDLDataSourceV2IdColumnMappingSuite
  extends ClusteredTableDDLWithIdColumnMapping
    with ClusteredTableDDLWithV2
    with ClusteredTableDDLWithColumnMappingV2
    with ClusteredTableDDLSuite

class ClusteredTableDDLDataSourceV2NameColumnMappingSuite
  extends ClusteredTableDDLWithNameColumnMapping
    with ClusteredTableDDLWithV2
    with ClusteredTableDDLWithColumnMappingV2
    with ClusteredTableDDLSuite
