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

import scala.collection.immutable.NumericRange

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.test.{DeltaExcludedTestMixin, DeltaSQLCommandTest}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.util.Utils

class CloneTableSQLSuite extends CloneTableSuiteBase
  with DeltaColumnMappingTestUtils
{
  // scalastyle:off argcount
  override protected def cloneTable(
      source: String,
      target: String,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      sourceFormat: String = "delta",
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): Unit = {
    val commandSql = CloneTableSQLTestUtils.buildCloneSqlString(
      source, target,
      sourceIsTable, targetIsTable,
      sourceFormat, targetLocation,
      versionAsOf, timestampAsOf,
      isCreate, isReplace, tableProperties)
    sql(commandSql)
  }
  // scalastyle:on argcount

  testAllClones(s"table version as of syntax") { (_, target, isShallow) =>
    val tbl = "source"
    testSyntax(
      tbl,
      target,
      s"CREATE TABLE delta.`$target` ${cloneTypeStr(isShallow)} CLONE $tbl VERSION AS OF 0"
    )
  }

  testAllClones("CREATE OR REPLACE syntax when there is no existing table") {
    (_, clone, isShallow) =>
      val tbl = "source"
      testSyntax(
        tbl,
        clone,
        s"CREATE OR REPLACE TABLE delta.`$clone` ${cloneTypeStr(isShallow)} CLONE $tbl"
      )
  }

  cloneTest("REPLACE cannot be used with IF NOT EXISTS") { (shallow, _) =>
    val tbl = "source"
    intercept[ParseException] {
      testSyntax(tbl, shallow,
        s"CREATE OR REPLACE TABLE IF NOT EXISTS delta.`$shallow` SHALLOW CLONE $tbl")
    }
    intercept[ParseException] {
      testSyntax(tbl, shallow,
        s"REPLACE TABLE IF NOT EXISTS delta.`$shallow` SHALLOW CLONE $tbl")
    }
  }

  testAllClones(
    "IF NOT EXISTS should not go through with CLONE if table exists") { (tblExt, _, isShallow) =>
    val sourceTable = "source"
    val conflictingTable = "conflict"
    withTable(sourceTable, conflictingTable) {
      sql(s"CREATE TABLE $conflictingTable " +
        s"USING PARQUET LOCATION '$tblExt' TBLPROPERTIES ('abc'='def', 'def'='ghi') AS SELECT 1")
      spark.range(5).write.format("delta").saveAsTable(sourceTable)

      sql(s"CREATE TABLE IF NOT EXISTS " +
        s"$conflictingTable ${cloneTypeStr(isShallow)} CLONE $sourceTable")

      checkAnswer(sql(s"SELECT COUNT(*) FROM $conflictingTable"), Row(1))
    }
  }

  testAllClones("IF NOT EXISTS should throw an error if path exists") { (_, target, isShallow) =>
    spark.range(5).write.format("delta").save(target)

    val ex = intercept[AnalysisException] {
      sql(s"CREATE TABLE IF NOT EXISTS " +
        s"delta.`$target` ${cloneTypeStr(isShallow)} CLONE delta.`$target`")
    }

    assert(ex.getMessage.contains("is not empty"))
  }

  cloneTest("Negative test: REPLACE table where there is no existing table") { (shallow, _) =>
    val tbl = "source"
    val ex = intercept[AnalysisException] {
      testSyntax(tbl, shallow, s"REPLACE TABLE delta.`$shallow` SHALLOW CLONE $tbl")
    }

    assert(ex.getMessage.contains("cannot be replaced as it does not exist."))
  }

  cloneTest("cloning a table that doesn't exist") { (tblExt, _) =>
    val ex = intercept[AnalysisException] {
      sql(s"CREATE TABLE delta.`$tblExt` SHALLOW CLONE not_exists")
    }
    assert(ex.getMessage.contains("Table not found") ||
      ex.getMessage.contains("The table or view `not_exists` cannot be found"))

    val ex2 = intercept[AnalysisException] {
      sql(s"CREATE TABLE delta.`$tblExt` SHALLOW CLONE not_exists VERSION AS OF 0")
    }
    assert(ex2.getMessage.contains("Table not found") ||
      ex2.getMessage.contains("The table or view `not_exists` cannot be found"))
  }

  cloneTest("cloning a view") { (tblExt, _) =>
    withTempView("tmp") {
      sql("CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM range(10)")
      val ex = intercept[AnalysisException] {
        sql(s"CREATE TABLE delta.`$tblExt` SHALLOW CLONE tmp")
      }
      assert(ex.errorClass === Some("DELTA_CLONE_UNSUPPORTED_SOURCE"))
      assert(ex.getMessage.contains("clone source 'tmp', whose format is View."))
    }
  }

  cloneTest("cloning a view over a Delta table") { (tblExt, _) =>
    withTable("delta_table") {
      withView("tmp") {
        sql("CREATE TABLE delta_table USING delta AS SELECT * FROM range(10)")
        sql("CREATE VIEW tmp AS SELECT * FROM delta_table")
        val ex = intercept[AnalysisException] {
          sql(s"CREATE TABLE delta.`$tblExt` SHALLOW CLONE tmp")
        }
        assert(ex.errorClass === Some("DELTA_CLONE_UNSUPPORTED_SOURCE"))
        assert(
          ex.getMessage.contains("clone source") &&
            ex.getMessage.contains("default.tmp', whose format is View.")
        )
      }
    }
  }

  cloneTest("check metrics returned from shallow clone", TAG_HAS_SHALLOW_CLONE) { (_, _) =>
    val source = "source"
    val target = "target"
    withTable(source, target) {
      spark.range(100).write.format("delta").saveAsTable(source)

      val res = sql(s"CREATE TABLE $target SHALLOW CLONE $source")

      // schema check
      val expectedColumns = Seq(
        "source_table_size",
        "source_num_of_files",
        "num_removed_files",
        "num_copied_files",
        "removed_files_size",
        "copied_files_size"
      )
      assert(expectedColumns == res.columns.toSeq)

      // logic check
      assert(res.count() == 1)
      val returnedMetrics = res.first()
      assert(returnedMetrics.getAs[Long]("source_table_size") != 0L)
      assert(returnedMetrics.getAs[Long]("source_num_of_files") != 0L)
      // Delta-OSS doesn't support copied file metrics
      assert(returnedMetrics.getAs[Long]("num_copied_files") == 0L)
      assert(returnedMetrics.getAs[Long]("copied_files_size") == 0L)
    }
  }

  cloneTest("Negative test: Clone to target path and also have external location") { (deep, ext) =>
    val sourceTable = "source"
    withTable(sourceTable) {
      spark.range(5).write.format("delta").saveAsTable(sourceTable)
      val ex = intercept[IllegalArgumentException] {
        runAndValidateClone(
          sourceTable,
          deep,
          sourceIsTable = true,
          targetLocation = Some(ext))()
      }

      assert(ex.getMessage.contains("Two paths were provided as the CLONE target"))
    }
  }
}


class CloneTableSQLIdColumnMappingSuite
  extends CloneTableSQLSuite
    with CloneTableColumnMappingSuiteBase
    with DeltaColumnMappingEnableIdMode {
}

class CloneTableSQLNameColumnMappingSuite
  extends CloneTableSQLSuite
    with CloneTableColumnMappingNameSuiteBase
    with DeltaColumnMappingEnableNameMode {
}

object CloneTableSQLTestUtils {

  // scalastyle:off argcount
  def buildCloneSqlString(
      source: String,
      target: String,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      sourceFormat: String = "delta",
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): String = {
    val header = if (isCreate && isReplace) {
      "CREATE OR REPLACE"
    } else if (isReplace) {
      "REPLACE"
    } else {
      "CREATE"
    }
    // e.g. CREATE TABLE targetTable
    val createTbl =
      if (targetIsTable) s"$header TABLE $target" else s"$header TABLE delta.`$target`"
    // e.g. CREATE TABLE targetTable SHALLOW CLONE
    val withMethod =
        createTbl + " SHALLOW CLONE "
    // e.g. CREATE TABLE targetTable SHALLOW CLONE delta.`/source/table`
    val withSource = if (sourceIsTable) {
      withMethod + s"$source "
    } else {
      withMethod + s"$sourceFormat.`$source` "
    }
    // e.g. CREATE TABLE targetTable SHALLOW CLONE delta.`/source/table` VERSION AS OF 0
    val withVersion = if (versionAsOf.isDefined) {
      withSource + s"VERSION AS OF ${versionAsOf.get}"
    } else if (timestampAsOf.isDefined) {
      withSource + s"TIMESTAMP AS OF '${timestampAsOf.get}'"
    } else {
      withSource
    }
    // e.g. CREATE TABLE targetTable SHALLOW CLONE delta.`/source/table` VERSION AS OF 0
    //      LOCATION '/desired/target/location'
    val withLocation = if (targetLocation.isDefined) {
      s" $withVersion LOCATION '${targetLocation.get}'"
    } else {
      withVersion
    }
    val withProperties = if (tableProperties.nonEmpty) {
      val props = tableProperties.map(p => s"'${p._1}' = '${p._2}'").mkString(",")
      s" $withLocation TBLPROPERTIES ($props)"
    } else {
      withLocation
    }
    withProperties
  }
  // scalastyle:on argcount
}

class CloneTableScalaDeletionVectorSuite
    extends CloneTableSQLSuite
    with DeltaSQLCommandTest
    with DeltaExcludedTestMixin
    with DeletionVectorsTestUtils {

  override def excluded: Seq[String] = super.excluded ++
    Seq(
      // These require the initial table protocol version to be low to work properly.
      "Cloning a table with new table properties that force protocol version upgrade -" +
        " delta.enableChangeDataFeed"
      , "Cloning a table with new table properties that force protocol version upgrade -" +
        " delta.enableDeletionVectors"
      , "Cloning a table without DV property should not upgrade protocol version"
      , "CLONE respects table features set by table property override, targetExists=true"
      , "CLONE ignores reader/writer session defaults")

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark.conf)
  }

  override protected def uniqueFileActionGroupBy(action: FileAction): String = {
    val filePath = action.pathAsUri.toString
    val dvId = action match {
      case add: AddFile => Option(add.deletionVector).map(_.uniqueId).getOrElse("")
      case remove: RemoveFile => Option(remove.deletionVector).map(_.uniqueId).getOrElse("")
      case _ => ""
    }
    filePath + dvId
  }

  testAllClones("Cloning table with persistent DVs") { (source, target, isShallow) =>
    // Create source table
    writeMultiFileSourceTable(
      source,
      fileRanges = Seq(0L until 30L, 30L until 60L, 60L until 90L))
    // Add DVs to 2 files, leave 1 file without DVs.
    spark.sql(s"DELETE FROM delta.`$source` WHERE id IN (24, 42)")
    runAndValidateCloneWithDVs(
      source,
      target,
      expectedNumFilesWithDVs = 2)
  }

  testAllClones("Cloning table with persistent DVs and absolute parquet paths"
  ) { (source, target, isShallow) =>
    withTempDir { originalSourceDir =>
      val originalSource = originalSourceDir.getCanonicalPath
      // Create source table, by writing to an upstream table and then shallow cloning before
      // adding DVs.
      writeMultiFileSourceTable(
        source = originalSource,
        fileRanges = Seq(0L until 30L, 30L until 60L, 60L until 90L))
      spark.sql(s"CREATE OR REPLACE TABLE delta.`$source` SHALLOW CLONE delta.`$originalSource`")
      // Add DVs to 2 files, leave 1 file without DVs.
      spark.sql(s"DELETE FROM delta.`$source` WHERE id IN (24, 42)")
      runAndValidateCloneWithDVs(
        source,
        target,
        expectedNumFilesWithDVs = 2)
    }
  }

  testAllClones("Cloning table with persistent DVs and absolute DV file paths"
  ) { (source, target, isShallow) =>
    withTempDir { originalSourceDir =>
      val originalSource = originalSourceDir.getCanonicalPath
      // Create source table, by writing to an upstream table, adding DVs and then shallow cloning.
      writeMultiFileSourceTable(
        source = originalSource,
        fileRanges = Seq(0L until 30L, 30L until 60L, 60L until 90L))
      // Add DVs to 2 files, leave 1 file without DVs.
      spark.sql(s"DELETE FROM delta.`$originalSource` WHERE id IN (24, 42)")
      val originalSourceTable = io.delta.tables.DeltaTable.forPath(spark, originalSource)
      spark.sql(s"CREATE OR REPLACE TABLE delta.`$source` SHALLOW CLONE delta.`$originalSource`")
      // Double check this clone was correct.
      checkAnswer(
        spark.read.format("delta").load(source), expectedAnswer = originalSourceTable.toDF)
      runAndValidateCloneWithDVs(
        source,
        target,
        expectedNumFilesWithDVs = 2)
    }
  }

  cloneTest("Shallow clone round-trip with DVs") { (source, target) =>
    // Create source table.
    writeMultiFileSourceTable(
      source = source,
      fileRanges = Seq(
        0L until 30L, // file 1
        30L until 60L, // file 2
        60L until 90L, //  file 3
        90L until 120L)) // file 4
    // Add DVs to files 1 and 2 and then shallow clone.
    spark.sql(s"DELETE FROM delta.`$source` WHERE id IN (24, 42)")
    runAndValidateCloneWithDVs(
      source = source,
      target = target,
      expectedNumFilesWithDVs = 2)

    // Add a new DV to file 3 and update the DV file 2,
    // leaving file 4 without a DV and file 1 with the existing DV.
    // Then shallow clone back into source.
    spark.sql(s"DELETE FROM delta.`$target` WHERE id IN (43, 69)")
    runAndValidateCloneWithDVs(
      source = target,
      target = source,
      expectedNumFilesWithDVs = 3,
      isReplaceOperation = true)
  }

  /** Write one file per range in `fileRanges`. */
  private def writeMultiFileSourceTable(
    source: String,
    fileRanges: Seq[NumericRange.Exclusive[Long]]): Unit = {
    for (range <- fileRanges) {
      spark.range(start = range.start, end = range.end, step = 1L, numPartitions = 1).toDF("id")
        .write.format("delta").mode("append").save(source)
    }
  }

  private def runAndValidateCloneWithDVs(
    source: String,
    target: String,
    expectedNumFilesWithDVs: Int,
    isReplaceOperation: Boolean = false): Unit = {
    val sourceDeltaLog = DeltaLog.forTable(spark, source)
    val targetDeltaLog = DeltaLog.forTable(spark, source)
    val filesWithDVsInSource = getFilesWithDeletionVectors(sourceDeltaLog)
    assert(filesWithDVsInSource.size === expectedNumFilesWithDVs)
    val numberOfUniqueDVFilesInSource = filesWithDVsInSource
      .map(_.deletionVector.pathOrInlineDv)
      .toSet
      .size

    runAndValidateClone(
      source,
      target,
      isReplaceOperation = isReplaceOperation)()
    val filesWithDVsInTarget = getFilesWithDeletionVectors(targetDeltaLog)
    val numberOfUniqueDVFilesInTarget = filesWithDVsInTarget
      .map(_.deletionVector.pathOrInlineDv)
      .toSet
      .size
    // Make sure we didn't accidentally copy some file multiple times.
    assert(numberOfUniqueDVFilesInSource === numberOfUniqueDVFilesInTarget)
    // Check contents of the copied DV files.
    val filesWithDVsInTargetByPath = filesWithDVsInTarget
      .map(addFile => addFile.path -> addFile)
      .toMap
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    for (sourceFile <- filesWithDVsInSource) {
      val targetFile = filesWithDVsInTargetByPath(sourceFile.path)
      if (sourceFile.deletionVector.isInline) {
        assert(targetFile.deletionVector.isInline)
        assert(sourceFile.deletionVector.inlineData === targetFile.deletionVector.inlineData)
      } else {
        def readDVData(path: Path): Array[Byte] = {
          val fs = path.getFileSystem(hadoopConf)
          val size = fs.getFileStatus(path).getLen
          val data = new Array[Byte](size.toInt)
          Utils.tryWithResource(fs.open(path)) { reader =>
            reader.readFully(data)
          }
          data
        }
        val sourceDVPath = sourceFile.deletionVector.absolutePath(sourceDeltaLog.dataPath)
        val targetDVPath = targetFile.deletionVector.absolutePath(targetDeltaLog.dataPath)
        val sourceData = readDVData(sourceDVPath)
        val targetData = readDVData(targetDVPath)
        assert(sourceData === targetData)
      }
    }
  }
}
