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

import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtils
import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaColumnMappingEnableIdMode, DeltaColumnMappingEnableNameMode, DeltaConfigs, DeltaLog, DeltaUnsupportedOperationException}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.SkippingEligibleDataType
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}

trait ClusteredTableCreateOrReplaceDDLSuiteBase
  extends QueryTest with SharedSparkSession with ClusteredTableTestUtils {

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
        "a, b"),
      (" multipart name",
        "a STRUCT<b INT, c STRING>, ts TIMESTAMP",
        "a.b, ts")
    ).foreach { case (testSuffix, columns, clusteringColumns) =>
      test(s"create/replace table$testSuffix") {
        withTable(testTable) {
          clauses.foreach { clause =>
            createOrReplaceClusteredTable(clause, testTable, columns, clusteringColumns)
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
                clause, targetTable, sourceTable, clusteringColumns, location = location)
              verifyClusteringColumns(targetTable, clusteringColumns, location)
            }
          }
        }
      }

      if (clauses.contains("REPLACE")) {
        test(s"Replace from non clustered table$testSuffix") {
          withTable(targetTable) {
            sql(s"CREATE TABLE $targetTable($columns) USING delta")
            createOrReplaceClusteredTable("REPLACE", targetTable, columns, clusteringColumns)
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
        val schemaStr = snapshot.statCollectionLogicalSchema.treeString
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
                  e,
                  "DELTA_CLUSTERING_COLUMN_MISSING_STATS"
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
        e,
        errorClass = "DELTA_CLUSTER_BY_INVALID_NUM_COLUMNS"
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
          e,
          errorClass = "DELTA_CLUSTER_BY_INVALID_NUM_COLUMNS"
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
      expectedLogicalClusteringColumns: String,
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
              e,
              "DELTA_CLUSTERING_COLUMN_MISSING_STATS"
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
                e,
                "DELTA_CLUSTERING_COLUMN_MISSING_STATS"
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
          e,
          "DELTA_CLUSTERING_COLUMN_MISSING_STATS"
        )
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
      "non-clustering column"
  )

  test("validate dropping clustering column is not allowed: single clustering column") {
    withClusteredTable(testTable, "col1 INT, col2 STRING, col3 LONG", "col1") {
      val e = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE $testTable DROP COLUMNS (col1)")
      }
      checkError(
        e,
        "DELTA_UNSUPPORTED_DROP_CLUSTERING_COLUMN"
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
        e,
        "DELTA_UNSUPPORTED_DROP_CLUSTERING_COLUMN"
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
        e,
        "DELTA_UNSUPPORTED_DROP_CLUSTERING_COLUMN"
      )
    }
  }
}

trait ClusteredTableDDLWithColumnMappingV2Base extends ClusteredTableDDLWithColumnMapping {
  test("test clustering column names (alter table + create table) with spaces") {
    withClusteredTable(testTable, "`col1 a` INT, col2 INT, col3 STRUCT<col4 INT, `col5 b` INT>, " +
      "`col6 c` STRUCT<col7 INT, `col8 d.e` INT>, `col9.f` INT", "`col1 a`") {
      val tableIdentifier = TableIdentifier(testTable)
      verifyClusteringColumns(tableIdentifier, "`col1 a`")

      // Test ALTER CLUSTER BY to change clustering columns away from names with spaces.
      sql(s"ALTER TABLE $testTable CLUSTER BY (col2)")
      verifyClusteringColumns(tableIdentifier, "col2")

      // Test ALTER CLUSTER BY to test with structs with spaces in varying places.
      sql(s"ALTER TABLE $testTable CLUSTER BY (col3.`col5 b`, `col6 c`.col7)")
      verifyClusteringColumns(tableIdentifier, "col3.`col5 b`, `col6 c`.col7")

      // Test ALTER CLUSTER BY on structs with spaces on both entries and with no spaces in the same
      // clustering spec, including cases where there is a '.' in the name.
      sql(s"ALTER TABLE $testTable CLUSTER BY (col3.col4, `col6 c`.`col8 d.e`, `col1 a`)")
      verifyClusteringColumns(tableIdentifier, "col3.col4,`col6 c`.`col8 d.e`,`col1 a`")

      // Test ALTER TABLE CLUSTER BY after renaming a column to include spaces in the name.
      sql(s"ALTER TABLE $testTable RENAME COLUMN col2 to `col2 e`")
      sql(s"ALTER TABLE $testTable CLUSTER BY (`col2 e`)")
      verifyClusteringColumns(tableIdentifier, "`col2 e`")

      // Test ALTER TABLE with '.' in the name.
      sql(s"ALTER TABLE $testTable CLUSTER BY (`col9.f`)")
      verifyClusteringColumns(tableIdentifier, "`col9.f`")
    }
  }
}

trait ClusteredTableDDLWithColumnMappingV2
  extends ClusteredTableDDLWithColumnMappingV2Base

trait ClusteredTableCreateOrReplaceDDLSuite
  extends ClusteredTableCreateOrReplaceDDLSuiteBase

trait ClusteredTableDDLSuiteBase
  extends ClusteredTableCreateOrReplaceDDLSuite
    with DeltaSQLCommandTest {

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
      verifyClusteringColumns(tableIdentifier, "id,name")

      // Change the clustering columns and verify that they are changed in both
      // Delta logs and catalog.
      sql(s"ALTER TABLE $testTable CLUSTER BY (name)")
      verifyClusteringColumns(tableIdentifier, "name")

      // Nested column scenario.
      sql(s"ALTER TABLE $testTable CLUSTER BY (a.b, id)")
      verifyClusteringColumns(tableIdentifier, "a.b,id")
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
      verifyClusteringColumns(tableIdentifier, "id")

      sql(s"ALTER TABLE $testTable CLUSTER BY NONE")
      verifyClusteringColumns(tableIdentifier, "")
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
        e2,
        "DELTA_CLUSTERING_WITH_ZORDER_BY"
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
              verifyClusteringColumns(testTable, "i", location)
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
          verifyClusteringColumns(TableIdentifier(testTable), "a")

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
              verifyClusteringColumns(testTable, "i", location)
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
      "a, b"),
    (" multipart name",
      "a STRUCT<b INT, c STRING>, ts TIMESTAMP",
      "a.b, ts")
  ).foreach { case (testSuffix, columns, clusteringColumns) =>
    test(s"create/replace table createOrReplace$testSuffix") {
      withTable(testTable) {
        // Repeat two times to test both create and replace cases.
        (1 to 2).foreach { _ =>
          createOrReplaceClusteredTable("CREATE OR REPLACE", testTable, columns, clusteringColumns)
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
              "CREATE OR REPLACE", targetTable, sourceTable, clusteringColumns, location = location)
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
    with ClusteredTableDDLSuite

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
