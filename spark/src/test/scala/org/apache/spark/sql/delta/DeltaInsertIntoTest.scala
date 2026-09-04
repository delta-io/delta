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

import scala.collection.mutable

// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.StructType

object DeltaInsertIntoTestHarness {
  sealed trait ExpectedResult[-T]
  object ExpectedResult {
    case class Success[T](expected: T) extends ExpectedResult[T]
    case class Failure[T](checkError: SparkThrowable => Unit = _ => ()) extends ExpectedResult[T]
  }
}

/**
 * There are **many** different ways to run an insert:
 * - Using SQL, the dataframe v1 and v2 APIs or the streaming API.
 * - Append vs. Overwrite / Partition overwrite.
 * - Position-based vs. name-based resolution.
 *
 * Each take a unique path through analysis. The abstractions below captures these different
 * inserts to allow more easily running tests with all or a subset of them.
 */
trait DeltaInsertIntoTestBase { self: AnyFunSuite =>

  protected def spark: SparkSession

  protected def checkAnswer(actual: => DataFrame, expected: Seq[Row]): Unit

  protected def checkExpectedError(
      exception: SparkThrowable,
      condition: String,
      sqlState: Option[String] = None,
      parameters: Map[String, String] = Map.empty): Unit = {
    assert(
      exception.getCondition == condition,
      s"Expected condition $condition but got ${exception.getCondition}")
    sqlState.foreach { expected =>
      assert(
        exception.getSqlState == expected,
        s"Expected SQLSTATE $expected but got ${exception.getSqlState}")
    }
    val actualParameters = exception.getMessageParameters
    assert(
      actualParameters.size == parameters.size &&
        parameters.forall { case (key, value) => actualParameters.get(key) == value },
      s"Expected parameters $parameters but got $actualParameters")
    assert(
      exception.getQueryContext.isEmpty,
      s"Expected no query context but got ${exception.getQueryContext.toSeq}")
  }

  protected def writeFormat: String

  protected val catalogName: String = "spark_catalog"

  protected def sourceTableName: String = "source"

  protected def targetTableName: String = "target"

  protected val DeltaSchemaAutoMigrateKey: String =
    "spark.databricks.delta.schema.autoMerge.enabled"

  private val ReplaceOnDataFrameWriterEnabledKey =
    "spark.databricks.delta.replaceOn.dataframe.writer.enabled"

  private def supportsInsertWithSchemaEvolutionSyntax: Boolean = {
    val versionParts = spark.version.split('.')
    val major = versionParts(0).toInt
    val minor = versionParts(1).toInt
    major > 4 || (major == 4 && minor >= 2)
  }

  private def targetTablePath: String =
    spark.sql(s"DESCRIBE DETAIL $quotedTargetTableName")
      .select("location").collect().head.getString(0)

  protected def withTestTables(tableNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      tableNames.reverse.foreach { tableName =>
        spark.sql(s"DROP TABLE IF EXISTS ${quoteMultipartIdentifier(tableName)}").collect()
      }
    }
  }

  protected def withRuntimeConf[T](pairs: (String, String)*)(f: => T): T = {
    val previousValues = pairs.map { case (key, _) => key -> spark.conf.getOption(key) }
    pairs.foreach { case (key, value) => spark.conf.set(key, value) }
    try f finally {
      previousValues.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  /**
   * Represents one way of inserting data into a Delta table.
   * @param name A human-readable name for the insert type displayed in the test names.
   * @param mode Append or Overwrite. This dictates in particular what the expected result after the
   *             insert should be.
   * @param byName Whether the insert uses name-based resolution or position-based resolution.
   * @param isSQL Whether the insert is done using SQL or the dataframe API (includes streaming
   *              write).
   */
  trait Insert {
    val name: String
    val mode: SaveMode
    val byName: Boolean
    val isSQL: Boolean

    /**
     * The method that tests will call to run the insert. Each type of insert must implement its
     * specific way to run insert.
     */
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit

    /** SQL keyword for this type of insert.  */
    def intoOrOverwrite: String = if (mode == SaveMode.Append) "INTO" else "OVERWRITE"

    /**
     * Runs a SQL INSERT, enabling Delta schema evolution when [[withSchemaEvolution]] is set.
     *
     * Spark 4.2 introduced the `INSERT WITH SCHEMA EVOLUTION` syntax. Earlier versions enable
     * schema evolution through the corresponding SQL configuration instead.
     *
     * @param buildInsert builds the INSERT statement given the schema evolution clause to splice
     *                    in right after the leading `INSERT` keyword.
     */
    def runInsertSql(withSchemaEvolution: Boolean)(buildInsert: String => String): Unit = {
      if (supportsInsertWithSchemaEvolutionSyntax) {
        val clause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION " else ""
        spark.sql(buildInsert(clause))
      } else {
        withRuntimeConf(DeltaSchemaAutoMigrateKey -> withSchemaEvolution.toString) {
          spark.sql(buildInsert(""))
        }
      }
    }

    /** The expected content of the table after the insert. */
    def expectedResult(initialDF: DataFrame, insertedDF: DataFrame): DataFrame = {
      // Always union with the initial data even if we're overwriting it to ensure the resulting
      // schema contains all columns from the table in case some are missing in `insertedDF`.
      val initial = if (mode == SaveMode.Overwrite) initialDF.limit(0) else initialDF
      initial.unionByName(insertedDF, allowMissingColumns = true)
    }
  }

  /** INSERT INTO/OVERWRITE */
  case class SQLInsertByPosition(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite"
    val byName: Boolean = false
    val isSQL: Boolean = true
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      runInsertSql(withSchemaEvolution) { clause =>
        s"INSERT $clause$intoOrOverwrite $quotedTargetTableName " +
          s"SELECT * FROM $quotedSourceTableName"
      }
    }
  }

  /** INSERT INTO/OVERWRITE (a, b) */
  case class SQLInsertColList(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite (columns) - $mode"
    val byName: Boolean = true
    val isSQL: Boolean = true
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      val colList = columns.mkString(", ")
      runInsertSql(withSchemaEvolution) { clause =>
        s"INSERT $clause$intoOrOverwrite $quotedTargetTableName ($colList) " +
          s"SELECT $colList FROM $quotedSourceTableName"
      }
    }
  }

  /** INSERT INTO/OVERWRITE BY NAME */
  case class SQLInsertByName(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite BY NAME - $mode"
    val byName: Boolean = true
    val isSQL: Boolean = true
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      runInsertSql(withSchemaEvolution) { clause =>
        s"INSERT $clause$intoOrOverwrite $quotedTargetTableName BY NAME " +
          s"SELECT ${columns.mkString(", ")} FROM $quotedSourceTableName"
      }
    }
  }

  /** INSERT INTO REPLACE WHERE */
  object SQLInsertOverwriteReplaceWhere extends Insert {
    val name: String = s"INSERT INTO REPLACE WHERE"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = false
    val isSQL: Boolean = true
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      runInsertSql(withSchemaEvolution) { clause =>
        s"INSERT ${clause}INTO $quotedTargetTableName " +
          s"REPLACE WHERE $whereCol = $whereValue " +
          s"SELECT ${columns.mkString(", ")} FROM $quotedSourceTableName"
      }
    }
  }

  /** INSERT OVERWRITE PARTITION (part = 1) */
  object SQLInsertOverwritePartitionByPosition extends Insert {
    val name: String = s"INSERT OVERWRITE PARTITION (partition)"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = false
    val isSQL: Boolean = true
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      val assignments = columns.filterNot(_ == whereCol).mkString(", ")
      runInsertSql(withSchemaEvolution) { clause =>
        s"INSERT ${clause}OVERWRITE $quotedTargetTableName " +
          s"PARTITION ($whereCol = $whereValue) " +
          s"SELECT $assignments FROM $quotedSourceTableName"
      }
    }
  }

  /** INSERT OVERWRITE PARTITION (part = 1) (a, b) */
  object SQLInsertOverwritePartitionColList extends Insert {
    val name: String = s"INSERT OVERWRITE PARTITION (partition) (columns)"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = true
    val isSQL: Boolean = true
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      val assignments = columns.filterNot(_ == whereCol).mkString(", ")
      runInsertSql(withSchemaEvolution) { clause =>
        s"INSERT ${clause}OVERWRITE $quotedTargetTableName " +
          s"PARTITION ($whereCol = $whereValue) ($assignments) " +
          s"SELECT $assignments FROM $quotedSourceTableName"
      }
    }
  }

  /** df.write.mode(mode).insertInto() */
  case class DFv1InsertInto(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 insertInto() - $mode"
    val byName: Boolean = false
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit =
      spark.read.table(sourceTableName).write.mode(mode)
        .option("mergeSchema", withSchemaEvolution.toString)
        .format(writeFormat)
        .insertInto(targetTableName)
  }

  /** df.write.mode(mode).saveAsTable() */
  case class DFv1SaveAsTable(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 saveAsTable() - $mode"
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      spark.read.table(sourceTableName).write.mode(mode)
        .option("mergeSchema", withSchemaEvolution.toString)
        .format(writeFormat)
        .saveAsTable(targetTableName)
    }
  }

  /** df.write.mode(mode).save() */
  case class DFv1Save(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 save() - $mode"
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      spark.read.table(sourceTableName).write.mode(mode)
        .option("mergeSchema", withSchemaEvolution.toString)
        .format(writeFormat)
        .save(targetTablePath)
    }
  }

  /** df.write.mode("overwrite").option("replaceOn", ...).insertInto() */
  object DFv1InsertIntoReplaceOn extends Insert {
    val name: String = "DFv1 insertInto() - REPLACE ON"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = false
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      withRuntimeConf(
          ReplaceOnDataFrameWriterEnabledKey -> "true") {
        spark.read.table(sourceTableName).write.mode(mode)
          .option("replaceOn", s"t.$whereCol = $whereValue")
          .option("targetAlias", "t")
          .option("mergeSchema", withSchemaEvolution.toString)
          .format(writeFormat)
          .insertInto(targetTableName)
      }
    }
  }

  /** df.write.mode("overwrite").option("replaceOn", ...).save() */
  object DFv1SaveReplaceOn extends Insert {
    val name: String = "DFv1 save() - REPLACE ON"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      withRuntimeConf(
          ReplaceOnDataFrameWriterEnabledKey -> "true") {
        spark.read.table(sourceTableName).write.mode(mode)
          .option("replaceOn", s"t.$whereCol = $whereValue")
          .option("targetAlias", "t")
          .option("mergeSchema", withSchemaEvolution.toString)
          .format(writeFormat)
          .save(targetTablePath)
      }
    }
  }

  /** df.write.mode(mode).option("partitionOverwriteMode", "dynamic").insertInto() */
  object DFv1InsertIntoDynamicPartitionOverwrite extends Insert {
    val name: String = s"DFv1 insertInto() - dynamic partition overwrite"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = false
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit =
      spark.read.table(sourceTableName).write
        .mode(mode)
        .option("partitionOverwriteMode", "dynamic")
        .option("mergeSchema", withSchemaEvolution.toString)
        .format(writeFormat)
        .insertInto(targetTableName)
  }

  /** df.writeTo.append() */
  object DFv2Append extends Insert { self: Insert =>
    val name: String = "DFv2 append()"
    val mode: SaveMode = SaveMode.Append
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      spark.read.table(sourceTableName)
        .writeTo(targetTableName)
        .option("mergeSchema", withSchemaEvolution.toString)
        .append()
    }
  }

  /** df.writeTo.overwrite() */
  object DFv2Overwrite extends Insert { self: Insert =>
    val name: String = s"DFv2 overwrite()"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      spark.read.table(sourceTableName)
        .writeTo(targetTableName)
        .option("mergeSchema", withSchemaEvolution.toString)
        .overwrite(col(whereCol) === lit(whereValue))
    }
  }

  /** df.writeTo.overwritePartitions() */
  object DFv2OverwritePartition extends Insert { self: Insert =>
    val name: String = s"DFv2 overwritePartitions()"
    override val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      spark.read.table(sourceTableName)
        .writeTo(targetTableName)
        .option("mergeSchema", withSchemaEvolution.toString)
        .overwritePartitions()
    }
  }

  /** df.writeStream.toTable() */
  object StreamingInsert extends Insert { self: Insert =>
    val name: String = s"Streaming toTable()"
    override val mode: SaveMode = SaveMode.Append
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(
        columns: Seq[String],
        whereCol: String,
        whereValue: Int,
        withSchemaEvolution: Boolean): Unit = {
      val checkpointLocation = s"$targetTablePath/_checkpoint"
      val query = spark.readStream
        .table(sourceTableName)
        .writeStream
        .option("checkpointLocation", checkpointLocation)
        .option("mergeSchema", withSchemaEvolution.toString)
        .format(writeFormat)
        .trigger(Trigger.AvailableNow())
        .toTable(targetTableName)
      try {
        query.processAllAvailable()
      } finally {
        query.stop()
      }
    }
  }

  /** Collects all the types of insert previously defined. */
  protected def allInsertTypes: Set[Insert] = Set(
        SQLInsertOverwriteReplaceWhere,
        SQLInsertOverwritePartitionByPosition,
        SQLInsertOverwritePartitionColList,
        DFv1InsertIntoDynamicPartitionOverwrite,
        DFv1InsertIntoReplaceOn,
        DFv1SaveReplaceOn,
        DFv2Append,
        DFv2Overwrite,
        DFv2OverwritePartition,
        StreamingInsert
  ) ++ (for {
      mode: SaveMode <- Seq(SaveMode.Append, SaveMode.Overwrite)
      insert: Insert <- Seq(
        SQLInsertByPosition(mode),
        SQLInsertColList(mode),
        SQLInsertByName(mode),
        DFv1InsertInto(mode),
        DFv1SaveAsTable(mode),
        DFv1Save(mode)
      )
    } yield insert).toSet

  protected def adaptExpectedResult(
      expectedResult: Any): DeltaInsertIntoTestHarness.ExpectedResult[Any] = {
    expectedResult match {
      case result: DeltaInsertIntoTestHarness.ExpectedResult[_] =>
        result.asInstanceOf[DeltaInsertIntoTestHarness.ExpectedResult[Any]]
      case result =>
        fail(s"Unsupported expected result type: ${result.getClass.getName}")
    }
  }

  protected def beforeStreamingInsert(): Unit = {}

  /** Collects inserts using resolution by name and by position respectively. */
  protected lazy val (insertsByName, insertsByPosition): (Set[Insert], Set[Insert]) =
    allInsertTypes.partition(_.byName)

  /** Collects inserts run through SQL and the dataframe API respectively. */
  protected lazy val (insertsSQL, insertsDataframe): (Set[Insert], Set[Insert]) =
    allInsertTypes.partition(_.isSQL)

  /** Collects append inserts vs. overwrite. */
  protected lazy val (insertsAppend, insertsOverwrite): (Set[Insert], Set[Insert]) =
    allInsertTypes.partition(_.mode == SaveMode.Append)

  /**
   * Collects inserts that don't support implicit casting: save() (all modes) and saveAsTable()
   * overwrite. These go through SaveIntoDataSourceCommand / ReplaceTableAsSelect which are not
   * handled by [[DeltaImplicitCast]]. Note that saveAsTable(Append) is NOT in this set because
   * it routes through AppendData (a V2WriteCommand) which IS handled by [[DeltaImplicitCast]].
   */
  protected lazy val insertsWithoutImplicitCastSupport: Set[Insert] = Set(
    DFv1Save(SaveMode.Append),
    DFv1Save(SaveMode.Overwrite),
    DFv1SaveReplaceOn,
    DFv1SaveAsTable(SaveMode.Overwrite)
  )


  /** Collects all test cases defined, aggregated by test name. Used in
   * [[checkAllTestCasesImplemented]] below to ensure each test covers all existing insert types.
   */
  protected val testCases: mutable.Map[String, Set[Insert]] =
    mutable.HashMap.empty.withDefaultValue(Set.empty)

  /** Tests should cover all insert types but it's easy to miss some cases. This method checks
   * that each test cover all insert types.
   */
  def checkAllTestCasesImplemented(ignoredTestCases: Map[String, Set[Insert]] = Map.empty): Unit = {
    val ignoredTests = ignoredTestCases.withDefaultValue(Set.empty)
    val missingTests = testCases.map {
      case (name, inserts) => name -> (allInsertTypes -- inserts -- ignoredTests(name))
    }.collect {
      case (name, missingInserts) if missingInserts.nonEmpty =>
        s"Test '$name' is not covering all insert types, missing: $missingInserts"
    }

    if (missingTests.nonEmpty) {
      fail("Missing test cases:\n" + missingTests)
    }
  }

  /** Convenience wrapper define test data using a SQL schema and a JSON string for each row. */
  case class TestData(schemaDDL: String, data: Seq[String]) {
    val schema: StructType = StructType.fromDDL(schemaDDL)
    def toDF: DataFrame = createDataFrameFromTestData(this)
  }

  protected def createDataFrameFromTestData(testData: TestData): DataFrame = {
    val schemaLiteral = quoteStringLiteral(testData.schemaDDL)
    val parsedRows = if (testData.data.nonEmpty) {
      val values =
        testData.data.map(value => s"(${quoteStringLiteral(value)})").mkString(", ")
        s"SELECT from_json(value, $schemaLiteral) AS parsed " +
          s"FROM VALUES $values AS json_data(value)"
    } else {
      s"SELECT from_json(CAST(NULL AS STRING), $schemaLiteral) AS parsed"
    }
    spark.sql(s"SELECT parsed.* FROM ($parsedRows) WHERE parsed IS NOT NULL")
  }

  protected def checkExpectedRows(
      actual: => DataFrame,
      insert: Insert,
      initialData: TestData,
      insertData: TestData,
      expectedSchema: StructType): Unit = {
    val expected = insert.expectedResult(initialData.toDF, insertData.toDF).collect().toSeq
    checkAnswer(actual, expected)
  }

  protected def checkExpectedRows(actual: => DataFrame, expectedData: TestData): Unit = {
    checkAnswer(actual, expectedData.toDF.collect().toSeq)
  }

  protected def createTableFromTestData(
      tableName: String,
      data: TestData,
      partitionBy: Seq[String] = Seq.empty): Unit = {
    val writer = data.toDF.write.format(writeFormat)
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    }
    writer.saveAsTable(tableName)
  }

  /**
   * Test runner to cover INSERT operations defined above.
   * @param name                Test name
   * @param initialData         Initial data used to create the table.
   * @param partitionBy         Partition columns for the initial table.
   * @param insertData          Additional data to be inserted.
   * @param overwriteWhere      Where clause for overwrite PARTITION / REPLACE WHERE (as
   *                            colName -> value)
   * @param expectedResult      Expected result, see
   *                            [[DeltaInsertIntoTestHarness.ExpectedResult]] above.
   * @param includeInserts      List of insert types to run the test with.
   *                            Defaults to all inserts.
   * @param excludeInserts      List of insert types to exclude when running the test.
   *                            Defaults to no  inserts excluded.
   * @param confs               Custom spark confs to set before running the insert
   *                            operation.
   * @param withSchemaEvolution Whether to enable Automatic Schema Evolution.
   */
  def testInserts(name: String)(
      initialData: TestData,
      partitionBy: Seq[String] = Seq.empty,
      insertData: TestData,
      overwriteWhere: (String, Int),
      expectedResult: Any,
      includeInserts: Set[Insert] = allInsertTypes,
      excludeInserts: Set[Insert] = Set.empty,
      confs: Seq[(String, String)] = Seq.empty,
      withSchemaEvolution: Boolean = false): Unit = {
    val inserts = includeInserts.filterNot(excludeInserts)
    assert(inserts.nonEmpty, s"Test '$name' doesn't cover any inserts. Please check the " +
      "includeInserts/excludeInserts sets and ensure at least one insert is included.")
    testCases(name) ++= inserts
    val adaptedExpectedResult = adaptExpectedResult(expectedResult)

    for (insert <- inserts) {
      test(s"${insert.name} - $name") {
        withTestTables(sourceTableName, targetTableName) {
          createTableFromTestData(targetTableName, initialData, partitionBy)
          // Write the data to insert to a table so that we can use it in both SQL and dataframe
          // writer inserts.
          createTableFromTestData(sourceTableName, insertData)

          def runInsert(): Unit =
            insert.runInsert(
              columns = insertData.schema.map(field => QuotingUtils.quoteIfNeeded(field.name)),
              whereCol = overwriteWhere._1,
              whereValue = overwriteWhere._2,
              withSchemaEvolution = withSchemaEvolution
            )

          withRuntimeConf(confs: _*) {
            adaptedExpectedResult match {
              case DeltaInsertIntoTestHarness.ExpectedResult.Success(
                    expectedSchema: StructType) =>
                runInsert()
                val target = spark.read.table(targetTableName)
                assert(target.schema === expectedSchema)
                checkExpectedRows(
                  target,
                  insert,
                  initialData,
                  insertData,
                  expectedSchema)
              case DeltaInsertIntoTestHarness.ExpectedResult.Success(expectedData: TestData) =>
                runInsert()
                val target = spark.read.table(targetTableName)
                assert(target.schema === expectedData.schema)
                checkExpectedRows(target, expectedData)
              case DeltaInsertIntoTestHarness.ExpectedResult.Success(expected) =>
                fail(s"Unsupported expected success type: ${expected.getClass.getName}")
              case DeltaInsertIntoTestHarness.ExpectedResult.Failure(checkError) =>
                val ex = if (insert == StreamingInsert) {
                  intercept[StreamingQueryException] {
                    runInsert()
                  }.getCause.asInstanceOf[SparkThrowable]
                } else {
                  intercept[SparkThrowable] {
                    runInsert()
                  }
                }
                checkError(ex)
            }
          }
        }
      }
    }
  }

  private def quotedSourceTableName: String = quoteMultipartIdentifier(sourceTableName)

  private def quotedTargetTableName: String = quoteMultipartIdentifier(targetTableName)

  protected def quoteIdentifier(identifier: String): String =
    s"`${identifier.replace("`", "``")}`"

  protected def quoteMultipartIdentifier(identifier: String): String =
    identifier.split("\\.", -1).map(quoteIdentifier).mkString(".")

  private def quoteStringLiteral(value: String): String =
    s"'${value.replace("'", "''")}'"
}
