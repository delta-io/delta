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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaExcludedTestMixin, DeltaSQLCommandTest}

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.errors.QueryExecutionErrors.toSQLType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy

class UpdateSQLSuite extends UpdateSuiteBase
  with DeltaSQLCommandTest {

  import testImplicits._

  test("explain") {
    append(Seq((2, 2)).toDF("key", "value"))
    val df = sql(s"EXPLAIN UPDATE delta.`$tempPath` SET key = 1, value = 2 WHERE key = 2")
    val outputs = df.collect().map(_.mkString).mkString
    assert(outputs.contains("Delta"))
    assert(!outputs.contains("index") && !outputs.contains("ActionLog"))
    // no change should be made by explain
    checkAnswer(readDeltaTable(tempPath), Row(2, 2))
  }

  test("SC-11376: Update command should check target columns during analysis, same key") {
    val targetDF = spark.read.json(
      """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}"""
        .split("\n").toSeq.toDS())

    testAnalysisException(
      targetDF,
      set = "z = 30" :: "z = 40" :: Nil,
      errMsgs = "There is a conflict from these SET columns" :: Nil)

    testAnalysisException(
      targetDF,
      set = "a.c.d = 'rand'" :: "a.c.d = 'RANDOM2'" :: Nil,
      errMsgs = "There is a conflict from these SET columns" :: Nil)
  }

  test("update a dataset temp view") {
    withTable("tab") {
      withTempView("v") {
        Seq((0, 3)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        spark.table("tab").as("name").createTempView("v")
        sql("UPDATE v SET key = 1 WHERE key = 0 AND value = 3")
        checkAnswer(spark.table("tab"), Row(1, 3))
      }
    }
  }

  test("update a SQL temp view") {
    withTable("tab") {
      withTempView("v") {
        Seq((0, 3)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        sql("CREATE TEMP VIEW v AS SELECT * FROM tab")
        QueryTest.checkAnswer(sql("UPDATE v SET key = 1 WHERE key = 0 AND value = 3"), Seq(Row(1)))
        checkAnswer(spark.table("tab"), Row(1, 3))
      }
    }
  }

  Seq(true, false).foreach { partitioned =>
    test(s"User defined _change_type column doesn't get dropped - partitioned=$partitioned") {
      withTable("tab") {
        sql(
          s"""CREATE TABLE tab USING DELTA
             |${if (partitioned) "PARTITIONED BY (part) " else ""}
             |TBLPROPERTIES (delta.enableChangeDataFeed = false)
             |AS SELECT id, int(id / 10) AS part, 'foo' as _change_type
             |FROM RANGE(1000)
             |""".stripMargin)
        val rowsToUpdate = (1 to 1000 by 42).mkString("(", ", ", ")")
        executeUpdate("tab", "_change_type = 'bar'", s"id in $rowsToUpdate")
        sql("SELECT id, _change_type FROM tab").collect().foreach { row =>
          val _change_type = row.getString(1)
          assert(_change_type === "foo" || _change_type === "bar",
            s"Invalid _change_type for id=${row.get(0)}")
        }
      }
    }
  }

  // The following two tests are run only against the SQL API because using the Scala API
  // incorrectly triggers the analyzer rule [[ResolveRowLevelCommandAssignments]] which allows
  // the casts without respecting the value of `storeAssignmentPolicy`.

  // Casts that are not valid upcasts (e.g. string -> boolean) are not allowed with
  // storeAssignmentPolicy = STRICT.
  test("invalid implicit cast string source type into boolean target, " +
   s"storeAssignmentPolicy = ${StoreAssignmentPolicy.STRICT}") {
    append(Seq((99, true), (100, false), (101, true)).toDF("key", "value"))
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString,
      DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false") {
    checkError(
      exception = intercept[AnalysisException] {
        executeUpdate(target = s"delta.`$tempPath`", set = "value = 'false'")
      },
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      parameters = Map(
        "expression" -> "'false'",
        "sourceType" -> toSQLType("STRING"),
        "targetType" -> toSQLType("BOOLEAN"),
        "details" -> ("The type path of the target object is:\n\nYou can either add an explicit " +
          "cast to the input data or choose a higher precision type of the field in the target " +
          "object")))
    }
  }

  // Implicit casts that are not upcasts are not allowed with storeAssignmentPolicy = STRICT.
  test("valid implicit cast string source type into int target, " +
     s"storeAssignmentPolicy = ${StoreAssignmentPolicy.STRICT}") {
    append(Seq((99, 2), (100, 4), (101, 3)).toDF("key", "value"))
    withSQLConf(
        SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString,
        DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key -> "false") {
    checkError(
      exception = intercept[AnalysisException] {
        executeUpdate(target = s"delta.`$tempPath`", set = "value = '5'")
      },
      errorClass = "CANNOT_UP_CAST_DATATYPE",
        parameters = Map(
        "expression" -> "'5'",
        "sourceType" -> toSQLType("STRING"),
        "targetType" -> toSQLType("INT"),
        "details" -> ("The type path of the target object is:\n\nYou can either add an explicit " +
          "cast to the input data or choose a higher precision type of the field in the target " +
          "object")))
    }
  }

  override protected def executeUpdate(
      target: String,
      set: String,
      where: String = null): Unit = {
    val whereClause = Option(where).map(c => s"WHERE $c").getOrElse("")
    sql(s"UPDATE $target SET $set $whereClause")
  }
}

class UpdateSQLWithDeletionVectorsSuite extends UpdateSQLSuite
  with DeltaExcludedTestMixin
  with DeletionVectorsTestUtils {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark, update = true)
  }

  override def excluded: Seq[String] = super.excluded ++
    Seq(
      // The following two tests must fail when DV is used. Covered by another test case:
      // "throw error when non-pinned TahoeFileIndex snapshot is used".
      "data and partition predicates - Partition=true Skipping=false",
      "data and partition predicates - Partition=false Skipping=false",
      // The scan schema contains additional row index filter columns.
      "schema pruning on finding files to update",
      "nested schema pruning on finding files to update"
    )

  test("repeated UPDATE produces deletion vectors") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val log = DeltaLog.forTable(spark, path)
      spark.range(0, 10, 1, numPartitions = 2).write.format("delta").save(path)

      // scalastyle:off argcount
      def updateAndCheckLog(
          where: String,
          expectedAnswer: Seq[Row],

          numAddFilesWithDVs: Int,
          sumNumRowsInAddFileWithDV: Int,
          sumNumRowsInAddFileWithoutDV: Int,
          sumDvCardinalityInAddFile: Long,

          numRemoveFilesWithDVs: Int,
          sumNumRowsInRemoveFileWithDV: Int,
          sumNumRowsInRemoveFileWithoutDV: Int,
          sumDvCardinalityInRemoveFile: Long): Unit = {
        executeUpdate(s"delta.`$path`", "id = -1", where)
        checkAnswer(sql(s"SELECT * FROM delta.`$path`"), expectedAnswer)

        val fileActions = log.getChanges(log.update().version).flatMap(_._2)
          .collect { case f: FileAction => f }
          .toSeq
        val addFiles = fileActions.collect { case f: AddFile => f }
        val removeFiles = fileActions.collect { case f: RemoveFile => f }

        val (addFilesWithDV, addFilesWithoutDV) = addFiles.partition(_.deletionVector != null)
        assert(addFilesWithDV.size === numAddFilesWithDVs)
        assert(
          addFilesWithDV.map(_.numPhysicalRecords.getOrElse(0L)).sum ===
            sumNumRowsInAddFileWithDV)
        assert(
          addFilesWithDV.map(_.deletionVector.cardinality).sum ===
            sumDvCardinalityInAddFile)
        assert(
          addFilesWithoutDV.map(_.numPhysicalRecords.getOrElse(0L)).sum ===
            sumNumRowsInAddFileWithoutDV)

        val (removeFilesWithDV, removeFilesWithoutDV) =
          removeFiles.partition(_.deletionVector != null)
        assert(removeFilesWithDV.size === numRemoveFilesWithDVs)
        assert(
          removeFilesWithDV.map(_.numPhysicalRecords.getOrElse(0L)).sum ===
            sumNumRowsInRemoveFileWithDV)
        assert(
          removeFilesWithDV.map(_.deletionVector.cardinality).sum ===
            sumDvCardinalityInRemoveFile)
        assert(
          removeFilesWithoutDV.map(_.numPhysicalRecords.getOrElse(0L)).sum ===
            sumNumRowsInRemoveFileWithoutDV)
      }
      // scalastyle:on argcount

      // DV created. 4 rows updated.
      updateAndCheckLog(
        "id % 3 = 0",
        Seq(-1, 1, 2, -1, 4, 5, -1, 7, 8, -1).map(Row(_)),
        numAddFilesWithDVs = 2,
        sumNumRowsInAddFileWithDV = 10,
        sumNumRowsInAddFileWithoutDV = 4,
        sumDvCardinalityInAddFile = 4,

        numRemoveFilesWithDVs = 0,
        sumNumRowsInRemoveFileWithDV = 0,
        sumNumRowsInRemoveFileWithoutDV = 10,
        sumDvCardinalityInRemoveFile = 0)

      // DV updated. 2 rows from the original file updated.
      updateAndCheckLog(
        "id % 4 = 0",
        Seq(-1, 1, 2, -1, -1, 5, -1, 7, -1, -1).map(Row(_)),
        numAddFilesWithDVs = 2,
        sumNumRowsInAddFileWithDV = 10,
        sumNumRowsInAddFileWithoutDV = 2,
        sumDvCardinalityInAddFile = 6,
        numRemoveFilesWithDVs = 2,
        sumNumRowsInRemoveFileWithDV = 10,
        sumNumRowsInRemoveFileWithoutDV = 0,
        sumDvCardinalityInRemoveFile = 4)

      // Original files DV removed, because all rows in the SECOND FILE are deleted.
      updateAndCheckLog(
        "id IN (5, 7)",
        Seq(-1, 1, 2, -1, -1, -1, -1, -1, -1, -1).map(Row(_)),
        numAddFilesWithDVs = 0,
        sumNumRowsInAddFileWithDV = 0,
        sumNumRowsInAddFileWithoutDV = 2,
        sumDvCardinalityInAddFile = 0,
        numRemoveFilesWithDVs = 1,
        sumNumRowsInRemoveFileWithDV = 5,
        sumNumRowsInRemoveFileWithoutDV = 0,
        sumDvCardinalityInRemoveFile = 3)
    }
  }

  test("UPDATE a whole partition do not produce DVs") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val log = DeltaLog.forTable(spark, path)
      spark.range(10).withColumn("part", col("id") % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .save(path)

      executeUpdate(s"delta.`$path`", "id = -1", where = "part = 0")
      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        Row(-1, 0) :: Row(1, 1) :: Row(-1, 0) ::
          Row(3, 1) :: Row(-1, 0) :: Row(5, 1) :: Row(-1, 0) ::
          Row(7, 1) :: Row(-1, 0) :: Row(9, 1) :: Nil)

      val fileActions = log.getChanges(log.update().version).flatMap(_._2)
        .collect { case f: FileAction => f }
        .toSeq
      val addFiles = fileActions.collect { case f: AddFile => f }
      val removeFiles = fileActions.collect { case f: RemoveFile => f }
      assert(addFiles.map(_.numPhysicalRecords.getOrElse(0L)).sum === 5)
      assert(removeFiles.map(_.numPhysicalRecords.getOrElse(0L)).sum === 5)
      for (a <- addFiles) assert(a.deletionVector === null)
    }
  }
}
