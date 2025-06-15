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

package org.apache.spark.sql.delta.rowid

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{col, lit}

trait RowTrackingMergeSuiteBase extends RowIdTestUtils
  with MergeHelpers {
  import testImplicits._

  protected def dvsEnabled: Boolean = true
  protected def cdfEnabled: Boolean = false

  protected val SOURCE_TABLE_NAME = "source"
  protected val TARGET_TABLE_NAME = "target"

  private val MANAGED_TABLE_NAMES = Seq(SOURCE_TABLE_NAME, TARGET_TABLE_NAME)

  protected val numRows = 4000
  protected val numUnmatchedRows = 2000

  // Source table with 4000 rows with 'key' 2000 until 6000.
  protected def createSourceTable(tableName: String, lastModifiedVersion: Long): Unit = {
    spark.range(start = numUnmatchedRows, end = numUnmatchedRows + numRows).toDF("key")
      .withColumn("stored_id", col("key"))
      .withColumn("last_modified_version", lit(lastModifiedVersion))
      .write.format("delta").saveAsTable(tableName)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey, value = "true")

    createSourceTable(SOURCE_TABLE_NAME, lastModifiedVersion = 1L)
  }

  override protected def afterAll(): Unit = {
    for (table <- MANAGED_TABLE_NAMES) {
      sql(s"DROP TABLE IF EXISTS $table")
    }
  }

  protected def withTestTable(
      tableName: String,
      partitionedTarget: Boolean,
      lastModifiedVersion: Long = 0L)(f: => Unit): Unit = {
    withTable(tableName) {
      val targetCreationDF = spark.range(end = numRows)
        .toDF("key")
        .withColumn("stored_id", col("key"))
        .withColumn("last_modified_version", lit(lastModifiedVersion))

      val clusteredTargetCreationWriter =
        if (partitionedTarget) {
          targetCreationDF
            .withColumn("partition", lit(0))
            .repartition(numPartitions = 2)
            .write
            .partitionBy("partition")
        } else {
          targetCreationDF
            .repartition(numPartitions = 2)
            .write
        }
      clusteredTargetCreationWriter.format("delta").saveAsTable(tableName)

      f
    }
  }

  protected def executeMerge(
      targetReference: String,
      sourceReference: String,
      clauses: MergeClause*): Unit = {
    val mergeSQL =
      s"""MERGE INTO $targetReference AS t
         |USING $sourceReference AS s
         |ON s.key = t.key
         |${clauses.map(_.sql).mkString("\n")}
         |""".stripMargin
    sql(mergeSQL)
  }

  /**
   * Create a test validating stable Row IDs and Row Commit Versions in MERGE. The test uses a fixed
   * source table and a modifiable target table. By default the source and the target table have
   * rows not matched in a join on 'key'.
   *
   *                  source table                                   target table
   *
   *                                                  |  key  | stored_id | last_modified_version |
   *                                                  |  0    |   0       |           0           |
   *                                                  |  1    |   1       |           0           |
   *                                                  |  ...  |   ...     |          ...          |
   *   |  key  | stored_id | last_modified_version |  |  1999 |   1999    |           0           |
   *   |  2000 |   2000    |           1           |  |  2000 |   2000    |           0           |
   *   |  2001 |   2001    |           1           |  |  2001 |   2001    |           0           |
   *   |  ...  |   ...     |          ...          |  |  ...  |   ...     |          ...          |
   *   |  3999 |   3999    |           1           |  |  3999 |   3999    |           0           |
   *   |  4000 |   4000    |           1           |
   *   |  ...  |   ...     |          ...          |
   *   |  5999 |   5999    |           1           |
   *
   * Tests also include CDF validation, which only works if 'key' is not changed in update clauses.
   */
  protected def rowTrackingMergeTests(name: String)(
    partitionedTarget: Boolean = false,
    targetAsView: Boolean = false,
    source: Option[String] = None,
    targetTablePostSetupAction: Option[() => Unit] = None,
    sqlConfs: Seq[(String, String)] = Seq.empty)(
    clauses: MergeClause*)(
    expected: Seq[Row],
    numFilesAfterMerge: Option[Int] = None): Unit = {
    test(name) {
      withTestTable(TARGET_TABLE_NAME, partitionedTarget) {
        // Post setup actions can be used to modify the target table, for example by inserting
        // more rows into it.
        targetTablePostSetupAction.foreach(_.apply())

        val preMergeRowIdMapping = getPreMergeRowIdMapping

        val sourceReference = source.getOrElse {
          if (partitionedTarget) {
            s"(SELECT *, 0 AS partition FROM $SOURCE_TABLE_NAME)"
          } else {
            SOURCE_TABLE_NAME
          }
        }

        val targetReference = if (targetAsView) {
          sql(s"CREATE TEMPORARY VIEW target_view AS SELECT * FROM $TARGET_TABLE_NAME")
          "target_view"
        } else {
          TARGET_TABLE_NAME
        }

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(TARGET_TABLE_NAME))
        withSQLConf(sqlConfs: _*) {
          checkRowTrackingMarkedAsPreservedForCommit(deltaLog) {
            checkFileActionInvariantBeforeAndAfterOperation(deltaLog) {
              checkHighWatermarkBeforeAndAfterOperation(deltaLog) {
                executeMerge(targetReference, sourceReference, clauses: _*)
              }
            }
          }
        }

        checkAnswer(sql(s"SELECT key, last_modified_version FROM $TARGET_TABLE_NAME"), expected)

        validateRowIdsPostMerge(preMergeRowIdMapping)
        validateRowCommitVersionsPostMerge()

        if (numFilesAfterMerge.isDefined) {
          assert(targetTableFiles().count() === numFilesAfterMerge.get,
            s"Expected ${numFilesAfterMerge.get} but got ${targetTableFiles().collect().mkString}")
        }

        if (cdfEnabled && targetTablePostSetupAction.isEmpty) {
          assert(deltaLog.update().version === 1, "Table has been modified more than once.")

          // Only read CDF from version 1 (the version after the MERGE)
          val cdfResult = CDCReader.changesToBatchDF(
              deltaLog,
              start = 1,
              end = 1,
              spark,
              useCoarseGrainedCDC = true)
              .select("stored_id",
                "key",
                "last_modified_version",
                "_change_type")
              .collect()

          val initialTableDf = spark.read.format("delta")
            .option("versionAsOf", 0)
            .table(TARGET_TABLE_NAME)
            .select("*", "_metadata.row_id")
            .alias("initial")

          val postMergeTableDf = spark.read.format("delta")
            .option("versionAsOf", 1)
            .table(TARGET_TABLE_NAME)
            .select("*", "_metadata.row_id")
            .alias("postMerge")

          // Outer join the table at the state before the merge with the state after the MERGE on
          // 'key' under the assumption that 'key' is not altered by the MERGE.
          val joinedInitialAndPost = initialTableDf
            .join(postMergeTableDf, usingColumn = "key", joinType = "fullouter")
            .select(
              "initial.key",
              "initial.stored_id",
              "initial.last_modified_version",
              "initial.row_id",
              "postMerge.key",
              "postMerge.stored_id",
              "postMerge.last_modified_version",
              "postMerge.row_id")
            .collect()

          joinedInitialAndPost.foreach {
            case Row(_, storedIdInitial, lastModifiedVersionInitial, rowIdInitial,
                _, storedIdPostMerge, lastModifiedVersionPostMerge, rowIdPostMerge) =>
              if (lastModifiedVersionPostMerge == null) { // Row has been deleted.
                val cdfEntries =
                  cdfResult.filter(row => row.getAs("stored_id") == storedIdInitial)

                assert(cdfEntries.length === 1,
                  s"Invalid number of CDF entries for deleted row with stored_id = " +
                    s"$storedIdInitial. ${cdfEntries.mkString}")
                assert(cdfEntries.head.getAs[String]("_change_type") === "delete",
                s"Invalid _change_type (!= delete) for inserted row with stored_id = " +
                  s" $storedIdInitial")
                assert(rowIdInitial.asInstanceOf[Long] < numRows,
                  "Row ID for delete row not from initial range")
              } else if (lastModifiedVersionInitial == null) { // Row has been inserted.
                val cdfEntries =
                  cdfResult.filter(row => row.getAs("stored_id") == storedIdPostMerge)

                assert(cdfEntries.length === 1,
                  s"Invalid number of CDF entries for inserted row with stored_id = " +
                    s" $storedIdPostMerge. ${cdfEntries.mkString}")
                assert(cdfEntries.head.getAs[String]("_change_type") === "insert",
                  s"Invalid _change_type (!= insert) for row with stored_id = $storedIdPostMerge")
                assert(rowIdPostMerge.asInstanceOf[Long] >= numRows,
                  "Row ID for inserted row from initial range")
              } else { // Row has been updated or is unchanged.
                val cdfEntries =
                  cdfResult.filter(row => row.getAs("stored_id") == storedIdPostMerge)

                if (lastModifiedVersionInitial != lastModifiedVersionPostMerge) {
                  // Row has been updated
                  assert(cdfEntries.length === 2,
                    s"Invalid number of CDF entries for updated/copied row with " +
                      s"stored_id = $storedIdPostMerge. ${cdfEntries.mkString}")
                } else { // Row is untouched or copied.
                  assert(Seq(0, 2).contains(cdfEntries.length),
                    s"Invalid number of CDF entries for updated/copied row with " +
                      s"stored_id = $storedIdPostMerge. ${cdfEntries.mkString}")
                }
              }
          }
        }
      }
    }
  }

  /**
   * This method retrieves the mapping of stored_id to row_id from the target table
   * before the merge operation.
   * It groups the collected data by stored_id and ensures that each stored_id
   * is associated with only row_id.
   *
   * @return A Map of stored_id to row_id before the merge operation.
   */
  protected def getPreMergeRowIdMapping: Map[Long, Long] = {
    spark.table(TARGET_TABLE_NAME)
      .select("stored_id", RowId.QUALIFIED_COLUMN_NAME)
      .as[(Long, Long)]
      .collect()
      .groupBy(_._1)
      .mapValues { values =>
        assert(values.length === 1)
        values.head._2
      }.toMap
  }

  /**
   * This method validates the row ids after the merge operation.
   * It ensures that the rows that existed before the merge
   * operation have kept their original row ids.
   * For the newly inserted rows, it checks that they have been assigned fresh row ids
   * that are greater than any row id before the merge operation.
   *
   * @param preMergeRowIdMapping The mapping of stored_id to row_id before the merge operation.
   */
  def validateRowIdsPostMerge(preMergeRowIdMapping: Map[Long, Long]): Unit = {
    val highestRowIdPreMerge = preMergeRowIdMapping.values.max

    val rowsAfterMerge = spark.read.table(TARGET_TABLE_NAME)
      .select("stored_id", RowId.QUALIFIED_COLUMN_NAME)
      .as[(Long, Long)]
      .collect()

    val (otherRows, insertedRows) =
      rowsAfterMerge.partition { case (storedId, _) => preMergeRowIdMapping.contains(storedId) }

    // Validate that rows kept their stable Row IDs.
    otherRows.foreach { case (storedId, rowId) =>
      assert(preMergeRowIdMapping(storedId) === rowId,
        s"Row ID has change for row with stored_id = $storedId")
    }

    assert(insertedRows.length === insertedRows.map { case (_, rowId) => rowId }.distinct.length,
      s"Row IDs are not unique for inserted rows: ${insertedRows.mkString}")

    // Validate that inserted rows received a fresh Row ID.
    insertedRows.foreach { case (storedId, rowId) =>
      assert(rowId > highestRowIdPreMerge,
        s"Row ID not fresh for inserted row with stored_id $storedId")
    }
  }

  /**
   * This method validates the row commit versions after the merge operation.
   * It ensures that the row commit version for each row in the target table
   * matches its last_modified_version.
   * This is to ensure that the row commit version is updated correctly during
   * the merge operation.
   */
  def validateRowCommitVersionsPostMerge(): Unit = {
    val rowsAfterMerge = spark.read.table(TARGET_TABLE_NAME)
      .select("stored_id", "last_modified_version", RowCommitVersion.QUALIFIED_COLUMN_NAME)
      .as[(Long, Long, Long)]
      .collect()

    rowsAfterMerge.foreach { case (storedId, lastModifiedVersion, rowCommitVersion) =>
      assert(rowCommitVersion === lastModifiedVersion,
        s"row commit version does not match for row with stored_id $storedId")
    }
  }

  private def targetTableFiles(): Dataset[AddFile] = {
    val targetTableIdentifier = TableIdentifier(TARGET_TABLE_NAME)
    DeltaLog.forTableWithSnapshot(spark, targetTableIdentifier)._2.allFiles
  }
}

trait RowTrackingMergeCommonTests extends RowTrackingMergeSuiteBase {

  rowTrackingMergeTests("INSERT NOT MATCHED only MERGE")()(
    clauses = insert("*"))(
    // The old rows that are in the target initially and the added rows.
    expected = (0 until numRows).map(Row(_, 0L))
      ++ (0 until numUnmatchedRows).map(id => Row(numRows + id, 1L))
  )

  rowTrackingMergeTests("DELETE MATCHED only MERGE")()(
    clauses = delete())(
    // All unmatched rows.
    expected = (0 until numUnmatchedRows).map(Row(_, 0L))
  )

  rowTrackingMergeTests("UPDATE MATCHED only MERGE")()(
    clauses = update("*"))(
    // Matched rows updated, other rows untouched.
    expected = (0 until numUnmatchedRows).map(Row(_, 0L))
      ++ (numUnmatchedRows until numRows).map(Row(_, 1L))
  )

  rowTrackingMergeTests("DELETE WHEN NOT MATCHED BY SOURCE only MERGE")()(
    clauses = deleteNotMatched())(
    // Unmatched rows only.
    expected = (numUnmatchedRows until numRows).map(Row(_, 0L))
  )

  rowTrackingMergeTests("UPDATE only WHEN NOT MATCHED BY SOURCE MERGE")()(
    clauses = updateNotMatched("last_modified_version = 1"))(
    // All rows not matched by source updated.
    expected = (0 until numUnmatchedRows).map(Row(_, 1L))
      ++ (numUnmatchedRows until numRows).map(Row(_, 0L))
  )

  rowTrackingMergeTests("UPDATE + DELETE WHEN NOT MATCHED BY SOURCE MERGE")()(
    clauses =
      deleteNotMatched("t.stored_id % 2 = 0"), updateNotMatched("last_modified_version = 1"))(
    expected = (0 until numUnmatchedRows).filter(_ % 2 == 1).map(Row(_, 1L)) ++
               (numUnmatchedRows until numRows).map(Row(_, 0L)))

  rowTrackingMergeTests("UPDATE only with source rows matching multiple target rows")(
    // Duplicate all target rows.
    targetTablePostSetupAction = Some(() => {
      spark.read.table(TARGET_TABLE_NAME)
        .withColumn("stored_id", col("stored_id") + numRows)
        .withColumn("last_modified_version", lit(1L))
        .write.mode("append").format("delta").saveAsTable(TARGET_TABLE_NAME) }))(
    clauses = update("t.last_modified_version = 2"))(
    // Updated 'key' and 'last_modified_version' for matched rows.
    expected = (0 until numUnmatchedRows).flatMap(id => Seq(Row(id, 0L), Row(id, 1L)))
      ++ (numUnmatchedRows until numRows).flatMap(id => Seq(Row(id, 2L), Row(id, 2L)))
  )

  rowTrackingMergeTests("DELETE only with source rows matching multiple target rows")(
    // Duplicate all target rows.
    targetTablePostSetupAction = Some(() => {
      spark.read.table(TARGET_TABLE_NAME)
        .withColumn("stored_id", col("stored_id") + numRows)
        .withColumn("last_modified_version", lit(1L))
        .write.mode("append").format("delta").saveAsTable(TARGET_TABLE_NAME) }))(
    clauses = delete())(
    // Deleted all matches (2 target rows per source row).
    expected = (0 until numUnmatchedRows).flatMap(id => Seq(Row(id, 0L), Row(id, 1L)))
  )

  rowTrackingMergeTests("Target is accessed through a view")(targetAsView = true)(
    clauses = update("*"))(
    expected = (0 until numUnmatchedRows).map(Row(_, 0L)) // Untouched.
      ++ (numUnmatchedRows until numRows).map(Row(_, 1L)) // Updated.
  )

  rowTrackingMergeTests("Optimized writes on partitioned table")(partitionedTarget = true)(
    clauses = update("*", "s.key % 2 = 0"), delete(), insert("*"), deleteNotMatched())(
    expected = (numUnmatchedRows.until(numRows, step = 2)).map(Row(_, 1L)) // Updated.
      ++ (numRows until numRows + numUnmatchedRows).map(Row(_, 1L)), // Inserted.
    numFilesAfterMerge = Some(1)
  )

  rowTrackingMergeTests("Optimized writes disabled on partitioned table")(
    partitionedTarget = true,
    sqlConfs = Seq(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false"))(
    clauses = update("*", "s.key % 2 = 0"), delete(), insert("*"), deleteNotMatched())(
    expected = (numUnmatchedRows.until(numRows, step = 2)).map(Row(_, 1L)) // Updated.
      ++ (numRows until numRows + numUnmatchedRows).map(Row(_, 1L)), // Inserted.
    numFilesAfterMerge = Some(1)
  )

  rowTrackingMergeTests("Optimized writes on un-partitioned table")(
    sqlConfs = Seq(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "true"))(
    clauses = update("*", "s.stored_id % 2 = 0"), delete(), insert("*"), deleteNotMatched())(
    expected = (numUnmatchedRows.until(numRows, step = 2)).map(Row(_, 1L)) // Updated.
        ++ (numRows until numRows + numUnmatchedRows).map(Row(_, 1L)), // Inserted.
    numFilesAfterMerge = Some(1)
  )

  rowTrackingMergeTests("Source and target referencing to the same table")(
    source = Some(s"(SELECT key, stored_id, 1L as last_modified_version FROM $TARGET_TABLE_NAME)"))(
    clauses = update("*"))(
    // All rows updated.
    expected = (0 until numRows).map(Row(_, 1L))
  )

  test("Multiple merges into the same table") {
    val numMerges = 5
    require(numMerges <= numUnmatchedRows)

    withTable(TARGET_TABLE_NAME) {
      // Create the target table using half the rows from the source table.
      spark.table(SOURCE_TABLE_NAME)
        .withColumn("last_modified_version", lit(0L))
        .filter("key % 2 = 0")
        .repartition(numPartitions = 2)
        .write
        .format("delta")
        .saveAsTable(TARGET_TABLE_NAME)

      val preMergeRowIdMapping = getPreMergeRowIdMapping

      // Give the target the same rows as the source table, one row at a time.
      for (i <- 0 until numMerges) {
        executeMerge(
          TARGET_TABLE_NAME,
          sourceReference = s"(SELECT ${numUnmatchedRows + i} AS key)",
          clauses =
            update(s"last_modified_version = ${i + 1}"),
            insert(s"(key, stored_id, last_modified_version) VALUES (key, key, ${i + 1})"))
      }

      checkAnswer(sql(s"SELECT key, last_modified_version FROM $TARGET_TABLE_NAME"),
        // Updated rows.
        (0 until numMerges).map(i => Row(numUnmatchedRows + i, i + 1))
          // Untouched rows.
          ++ (numUnmatchedRows + numMerges + 1).until(numRows + numUnmatchedRows, step = 2)
            .map(Row(_, 0L))
      )

      validateRowIdsPostMerge(preMergeRowIdMapping)
      validateRowCommitVersionsPostMerge()
    }
  }

  test("Row tracking marked as not preserved when row tracking disabled") {
    withRowTrackingEnabled(enabled = false) {
      withTestTable(TARGET_TABLE_NAME, partitionedTarget = false) {
        val log = DeltaLog.forTable(spark, TableIdentifier(TARGET_TABLE_NAME))
        assert(!rowTrackingMarkedAsPreservedForCommit(log) {
          executeMerge(
            TARGET_TABLE_NAME,
            SOURCE_TABLE_NAME,
            clauses = update("*"), insert("*"))
        })
      }
    }
  }

  test("Row tracking marked as not preserved when row tracking is supported but disabled") {
    withRowTrackingEnabled(enabled = false) {
      withTestTable(TARGET_TABLE_NAME, partitionedTarget = false) {
        sql(
          s"""
             |ALTER TABLE $TARGET_TABLE_NAME
             |SET TBLPROPERTIES (
             |'$rowTrackingFeatureName' = 'supported',
             |'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION)""".stripMargin)

        val log = DeltaLog.forTable(spark, TableIdentifier(TARGET_TABLE_NAME))
        assert(!rowTrackingMarkedAsPreservedForCommit(log) {
          executeMerge(
            TARGET_TABLE_NAME,
            SOURCE_TABLE_NAME,
            clauses = update("*"), insert("*"))
        })
      }
    }
  }

  test("schema evolution, extra nested column in source - update") {
    withTable(TARGET_TABLE_NAME) {
      import testImplicits._
      val targetData = Seq((0L, 0L, 0L, (1, 10)), (1L, 1L, 0L, (2, 2000)))
        .toDF("key", "stored_id", "last_modified_version", "x")
        .selectExpr(
          "key",
          "stored_id",
          "last_modified_version",
          "named_struct('a', x._1, 'c', x._2) as x")
      targetData.repartition(1).write.format("delta").saveAsTable(TARGET_TABLE_NAME)

      val sourceData = Seq((0L, 0L, 1L, (10, 100, 10000)))
        .toDF("key", "stored_id", "last_modified_version", "x")
        .selectExpr(
          "key",
          "stored_id",
          "last_modified_version",
          "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x")

      val preMergeRowIdMapping = getPreMergeRowIdMapping
      withTempView("src") {
        sourceData.createOrReplaceTempView("src")
        withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
          executeMerge(
            TARGET_TABLE_NAME,
            sourceReference = "src",
            clauses = update("*"))
        }
      }
      checkAnswer(sql(s"SELECT stored_id, last_modified_version FROM $TARGET_TABLE_NAME"),
        Seq(Row(0L, 1L), Row(1L, 0L)))
      validateRowIdsPostMerge(preMergeRowIdMapping)
      validateRowCommitVersionsPostMerge()
    }
  }

  test("MERGE preserves Row Tracking on tables enabled using backfill") {
    withSQLConf(DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_ENABLED.key -> "true") {
      val SOURCE_TABLE_NAME_FOR_BACKFILL_TEST = "backfilled_source"
      withTable(SOURCE_TABLE_NAME_FOR_BACKFILL_TEST) {
        createSourceTable(SOURCE_TABLE_NAME_FOR_BACKFILL_TEST, lastModifiedVersion = 4L)

        withRowTrackingEnabled(enabled = false) {
          withTestTable(TARGET_TABLE_NAME, partitionedTarget = false, lastModifiedVersion = 2L) {
            val (log, snapshot) =
              DeltaLog.forTableWithSnapshot(spark, TableIdentifier(TARGET_TABLE_NAME))
            assert(!RowTracking.isEnabled(snapshot.protocol, snapshot.metadata))
            validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
              triggerBackfillOnTestTableUsingAlterTable(TARGET_TABLE_NAME, numRows, log)
            }
            val preMergeRowIdMapping = getPreMergeRowIdMapping

            executeMerge(
              TARGET_TABLE_NAME,
              SOURCE_TABLE_NAME_FOR_BACKFILL_TEST,
              clauses = update("*"), insert("*"))

            validateRowIdsPostMerge(preMergeRowIdMapping)
            validateRowCommitVersionsPostMerge()
          }
        }
      }
    }
  }
}

trait RowTrackingMergeDVTests extends RowTrackingMergeSuiteBase
  with DeletionVectorsTestUtils {

  override protected def dvsEnabled: Boolean = true

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark, delete = true, update = true, merge = true)
  }
}

trait RowTrackingMergeCDFTests extends RowTrackingMergeSuiteBase {

  override def cdfEnabled: Boolean = true

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, value = true)
  }
}

trait RowTrackingMergeWithoutDVTests extends RowTrackingMergeCommonTests {

  override protected def dvsEnabled: Boolean = false

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey,
      value = "false")
    spark.conf.set(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key, value = false)
  }
}

class RowTrackingMergeSuite extends RowTrackingMergeCommonTests

class RowTrackingMergeDVSuite extends RowTrackingMergeSuite
  with RowTrackingMergeDVTests

class RowTrackingMergeCDFSuite extends RowTrackingMergeSuite
  with RowTrackingMergeCDFTests

class RowTrackingMergeCDFDVSuite extends RowTrackingMergeSuite
  with RowTrackingMergeDVTests with RowTrackingMergeCDFTests

class RowTrackingMergeWithoutDVSuite extends RowTrackingMergeCommonTests
  with RowTrackingMergeWithoutDVTests

class RowTrackingMergeWithoutDVCDFSuite extends RowTrackingMergeCommonTests
  with RowTrackingMergeWithoutDVTests with RowTrackingMergeCDFTests

class RowTrackingMergeColumnNameMappingSuite extends RowTrackingMergeCommonTests
  with DeltaColumnMappingEnableNameMode

class RowTrackingMergeColumnIdMappingSuite extends RowTrackingMergeCommonTests
  with DeltaColumnMappingEnableIdMode

class RowTrackingMergeColumnNameMappingCDFDVSuite extends RowTrackingMergeCommonTests
  with DeltaColumnMappingEnableNameMode with RowTrackingMergeDVTests with RowTrackingMergeCDFTests

class RowTrackingMergeColumnIdMappingCDFDVSuite extends RowTrackingMergeCommonTests
  with DeltaColumnMappingEnableIdMode with RowTrackingMergeDVTests with RowTrackingMergeCDFTests
