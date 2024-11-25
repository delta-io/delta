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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.{DebugFilesystem, SparkThrowable}
import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.StructType

/**
 * There are **many** different ways to run an insert:
 * - Using SQL, the dataframe v1 and v2 APIs or the streaming API.
 * - Append vs. Overwrite / Partition overwrite.
 * - Position-based vs. name-based resolution.
 *
 * Each take a unique path through analysis. The abstractions below captures these different
 * inserts to allow more easily running tests with all or a subset of them.
 */
trait DeltaInsertIntoTest extends QueryTest with DeltaDMLTestUtils with DeltaSQLCommandTest {

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
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit

    /** SQL keyword for this type of insert.  */
    def intoOrOverwrite: String = if (mode == SaveMode.Append) "INTO" else "OVERWRITE"

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
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      sql(s"INSERT $intoOrOverwrite target SELECT * FROM source")
  }

  /** INSERT INTO/OVERWRITE (a, b) */
  case class SQLInsertColList(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite (columns) - $mode"
    val byName: Boolean = true
    val isSQL: Boolean = true
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val colList = columns.mkString(", ")
      sql(s"INSERT $intoOrOverwrite target ($colList) SELECT $colList FROM source")
    }
  }

  /** INSERT INTO/OVERWRITE BY NAME */
  case class SQLInsertByName(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite BY NAME - $mode"
    val byName: Boolean = true
    val isSQL: Boolean = true
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      sql(s"INSERT $intoOrOverwrite target BY NAME SELECT ${columns.mkString(", ")} FROM source")
  }

  /** INSERT INTO REPLACE WHERE */
  object SQLInsertOverwriteReplaceWhere extends Insert {
    val name: String = s"INSERT INTO REPLACE WHERE"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = false
    val isSQL: Boolean = true
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      sql(s"INSERT INTO target REPLACE WHERE $whereCol = $whereValue " +
          s"SELECT ${columns.mkString(", ")} FROM source")
  }

  /** INSERT OVERWRITE PARTITION (part = 1) */
  object SQLInsertOverwritePartitionByPosition extends Insert {
    val name: String = s"INSERT OVERWRITE PARTITION (partition)"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = false
    val isSQL: Boolean = true
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val assignments = columns.filterNot(_ == whereCol).mkString(", ")
      sql(s"INSERT OVERWRITE target PARTITION ($whereCol = $whereValue) " +
          s"SELECT $assignments FROM source")
    }
  }

  /** INSERT OVERWRITE PARTITION (part = 1) (a, b) */
  object SQLInsertOverwritePartitionColList extends Insert {
    val name: String = s"INSERT OVERWRITE PARTITION (partition) (columns)"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = true
    val isSQL: Boolean = true
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val assignments = columns.filterNot(_ == whereCol).mkString(", ")
      sql(s"INSERT OVERWRITE target PARTITION ($whereCol = $whereValue) ($assignments) " +
          s"SELECT $assignments FROM source")
    }
  }

  /** df.write.mode(mode).insertInto() */
  case class DFv1InsertInto(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 insertInto() - $mode"
    val byName: Boolean = false
    val isSQL: Boolean = false
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      spark.read.table("source").write.mode(mode).insertInto("target")
  }

  /** df.write.mode(mode).saveAsTable() */
  case class DFv1SaveAsTable(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 saveAsTable() - $mode"
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").write.mode(mode).format("delta").saveAsTable("target")
    }
  }

  /** df.write.mode(mode).save() */
  case class DFv1Save(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 save() - $mode"
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("target"))
      spark.read.table("source").write.mode(mode).format("delta").save(deltaLog.dataPath.toString)
    }
  }

  /** df.write.mode(mode).option("partitionOverwriteMode", "dynamic").insertInto() */
  object DFv1InsertIntoDynamicPartitionOverwrite extends Insert {
    val name: String = s"DFv1 insertInto() - dynamic partition overwrite"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = false
    val isSQL: Boolean = false
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      spark.read.table("source").write
        .mode(mode)
        .option("partitionOverwriteMode", "dynamic")
        .insertInto("target")
  }

  /** df.writeTo.append() */
  object DFv2Append extends Insert { self: Insert =>
    val name: String = "DFv2 append()"
    val mode: SaveMode = SaveMode.Append
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").writeTo("target").append()
    }
  }

  /** df.writeTo.overwrite() */
  object DFv2Overwrite extends Insert { self: Insert =>
    val name: String = s"DFv2 overwrite()"
    val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").writeTo("target").overwrite(col(whereCol) === lit(whereValue))
    }
  }

  /** df.writeTo.overwritePartitions() */
  object DFv2OverwritePartition extends Insert { self: Insert =>
    val name: String = s"DFv2 overwritePartitions()"
    override val mode: SaveMode = SaveMode.Overwrite
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").writeTo("target").overwritePartitions()
    }
  }

  /** df.writeStream.toTable() */
  object StreamingInsert extends Insert { self: Insert =>
    val name: String = s"Streaming toTable()"
    override val mode: SaveMode = SaveMode.Append
    val byName: Boolean = true
    val isSQL: Boolean = false
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val tablePath = DeltaLog.forTable(spark, TableIdentifier("target")).dataPath
      val query = spark.readStream
        .table("source")
        .writeStream
        .option("checkpointLocation", tablePath.toString)
        .format("delta")
        .trigger(Trigger.AvailableNow())
        .toTable("target")
      query.processAllAvailable()
    }
  }

  /** Collects all the types of insert previously defined. */
  protected lazy val allInsertTypes: Set[Insert] = Set(
        SQLInsertOverwriteReplaceWhere,
        SQLInsertOverwritePartitionByPosition,
        SQLInsertOverwritePartitionColList,
        DFv1InsertIntoDynamicPartitionOverwrite,
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

  /** Collects inserts using resolution by name and by position respectively. */
  protected lazy val (insertsByName, insertsByPosition): (Set[Insert], Set[Insert]) =
    allInsertTypes.partition(_.byName)

  /** Collects inserts run through SQL and the dataframe API respectively. */
  protected lazy val (insertsSQL, insertsDataframe): (Set[Insert], Set[Insert]) =
    allInsertTypes.partition(_.isSQL)

  /** Collects append inserts vs. overwrite. */
  protected lazy val (insertsAppend, insertsOverwrite): (Set[Insert], Set[Insert]) =
    allInsertTypes.partition(_.mode == SaveMode.Append)

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
    def toDF: DataFrame = readFromJSON(data, schema)
  }

  /**
   * Test runner to cover INSERT operations defined above.
   * @param name           Test name
   * @param initialData    Initial data used to create the table.
   * @param partitionBy    Partition columns for the initial table.
   * @param insertData     Additional data to be inserted.
   * @param overwriteWhere Where clause for overwrite PARTITION / REPLACE WHERE (as
   *                       colName -> value)
   * @param expectedResult Expected result, see [[ExpectedResult]] above.
   * @param includeInserts List of insert types to run the test with. Defaults to all inserts.
   * @param excludeInserts List of insert types to exclude when running the test. Defaults to no
   *                       inserts excluded.
   * @param confs          Custom spark confs to set before running the insert operation.
   */
  def testInserts[T](name: String)(
      initialData: TestData,
      partitionBy: Seq[String] = Seq.empty,
      insertData: TestData,
      overwriteWhere: (String, Int),
      expectedResult: ExpectedResult[T],
      includeInserts: Set[Insert] = allInsertTypes,
      excludeInserts: Set[Insert] = Set.empty,
      confs: Seq[(String, String)] = Seq.empty): Unit = {
    val inserts = includeInserts.filterNot(excludeInserts)
    assert(inserts.nonEmpty, s"Test '$name' doesn't cover any inserts. Please check the " +
      "includeInserts/excludeInserts sets and ensure at least one insert is included.")
    testCases(name) ++= inserts

    for (insert <- inserts) {
      test(s"${insert.name} - $name") {
        withTable("source", "target") {
          val writer = initialData.toDF.write.format("delta")
          if (partitionBy.nonEmpty) {
            writer.partitionBy(partitionBy: _*)
          }
          writer.saveAsTable("target")
          // Write the data to insert to a table so that we can use it in both SQL and dataframe
          // writer inserts.
          insertData.toDF.write.format("delta").saveAsTable("source")

          def runInsert(): Unit =
            insert.runInsert(
              columns = insertData.schema.map(_.name),
              whereCol = overwriteWhere._1,
              whereValue = overwriteWhere._2
            )

          withSQLConf(confs: _*) {
            expectedResult match {
              case ExpectedResult.Success(expectedSchema: StructType) =>
                runInsert()
                val target = spark.read.table("target")
                assert(target.schema === expectedSchema)
                checkAnswer(target, insert.expectedResult(initialData.toDF, insertData.toDF))
              case ExpectedResult.Success(expectedData: TestData) =>
                runInsert()
                val target = spark.read.table("target")
                assert(target.schema === expectedData.schema)
                checkAnswer(spark.read.table("target"), expectedData.toDF)
              case ExpectedResult.Failure(checkError) =>
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
}
