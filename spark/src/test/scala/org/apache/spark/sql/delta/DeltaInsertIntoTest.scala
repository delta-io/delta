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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.{DebugFilesystem, SparkThrowable}
import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.Trigger
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
   * @param mode Append or Overwrite. This dictates in particular what the expected result after the
   *             insert should be.
   * @param name A human-readable name for the insert type displayed in the test names.
   */
  trait Insert {
    val mode: SaveMode
    val name: String

    /**
     * The method that tests will call to run the insert. Each type of insert must implement its
     * specific way to run insert.
     */
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit

    /** SQL keyword for this type of insert.  */
    def intoOrOverwrite: String = if (mode == SaveMode.Append) "INTO" else "OVERWRITE"

    /** The expected content of the table after the insert. */
    def expectedResult(initialDF: DataFrame, insertedDF: DataFrame): DataFrame =
      if (mode == SaveMode.Overwrite) insertedDF
      else initialDF.unionByName(insertedDF, allowMissingColumns = true)
  }

  /** INSERT INTO/OVERWRITE */
  case class SQLInsertByPosition(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      sql(s"INSERT $intoOrOverwrite target SELECT * FROM source")
  }

  /** INSERT INTO/OVERWRITE (a, b) */
  case class SQLInsertColList(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite (columns) - $mode"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val colList = columns.mkString(", ")
      sql(s"INSERT $intoOrOverwrite target ($colList) SELECT $colList FROM source")
    }
  }

  /** INSERT INTO/OVERWRITE BY NAME */
  case class SQLInsertByName(mode: SaveMode) extends Insert {
    val name: String = s"INSERT $intoOrOverwrite BY NAME - $mode"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      sql(s"INSERT $intoOrOverwrite target SELECT ${columns.mkString(", ")} FROM source")
  }

  /** INSERT INTO REPLACE WHERE */
  object SQLInsertOverwriteReplaceWhere extends Insert {
    val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"INSERT INTO REPLACE WHERE"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      sql(s"INSERT INTO target REPLACE WHERE $whereCol = $whereValue " +
          s"SELECT ${columns.mkString(", ")} FROM source")
  }

  /** INSERT OVERWRITE PARTITION (part = 1) */
  object SQLInsertOverwritePartitionByPosition extends Insert {
    val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"INSERT OVERWRITE PARTITION (partition)"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val assignments = columns.filterNot(_ == whereCol).mkString(", ")
      sql(s"INSERT OVERWRITE target PARTITION ($whereCol = $whereValue) " +
          s"SELECT $assignments FROM source")
    }
  }

  /** INSERT OVERWRITE PARTITION (part = 1) (a, b) */
  object SQLInsertOverwritePartitionColList extends Insert {
    val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"INSERT OVERWRITE PARTITION (partition) (columns)"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val assignments = columns.filterNot(_ == whereCol).mkString(", ")
      sql(s"INSERT OVERWRITE target PARTITION ($whereCol = $whereValue) ($assignments) " +
          s"SELECT $assignments FROM source")
    }
  }

  /** df.write.mode(mode).insertInto() */
  case class DFv1InsertInto(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 insertInto() - $mode"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit =
      spark.read.table("source").write.mode(mode).insertInto("target")
  }

  /** df.write.mode(mode).saveAsTable() */
  case class DFv1SaveAsTable(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 saveAsTable() - $mode"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").write.mode(mode).format("delta").saveAsTable("target")
    }
  }

  /** df.write.mode(mode).save() */
  case class DFv1Save(mode: SaveMode) extends Insert {
    val name: String = s"DFv1 save() - $mode"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("target"))
      spark.read.table("source").write.mode(mode).format("delta").save(deltaLog.dataPath.toString)
    }
  }

  /** df.writeTo.append() */
  object DFv2Append extends Insert { self: Insert =>
    val mode: SaveMode = SaveMode.Append
    val name: String = "DFv2 append()"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").writeTo("target").append()
    }
  }

  /** df.writeTo.overwrite() */
  object DFv2Overwrite extends Insert { self: Insert =>
    val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"DFv2 overwrite()"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").writeTo("target").overwrite(col(whereCol) === lit(whereValue))
    }
  }

  /** df.writeTo.overwritePartitions() */
  object DFv2OverwritePartition extends Insert { self: Insert =>
    override val mode: SaveMode = SaveMode.Overwrite
    val name: String = s"DFv2 overwritePartitions()"
    def runInsert(columns: Seq[String], whereCol: String, whereValue: Int): Unit = {
      spark.read.table("source").writeTo("target").overwritePartitions()
    }
  }

  /** df.writeStream.toTable() */
  object StreamingInsert extends Insert { self: Insert =>
    override val mode: SaveMode = SaveMode.Append
    val name: String = s"Streaming toTable()"
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
  protected lazy val allInsertTypes: Seq[Insert] = Seq(
        SQLInsertOverwriteReplaceWhere,
        SQLInsertOverwritePartitionByPosition,
        SQLInsertOverwritePartitionColList,
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
    } yield insert)

  /**
   * Represents the expected result after running an insert operation in `testInserts()` below.
   * Either:
   * - Success: the table schema after the operation is checked against the expected schema.
   *   `testInserts()` also validates the data, though it's able to infer the expected data from the
   *   test inputs.
   * - Failure: an exception is thrown and the caller passes a function to check that it matches an
   *   expected error.
   */
  type ExpectedResult = Either[StructType, SparkThrowable => Unit]
  object ExpectedResult {
    def Success(expectedSchema: StructType): ExpectedResult = Left(expectedSchema)
    def Failure(checkError: SparkThrowable => Unit): ExpectedResult = Right(checkError)
  }

  /**
   * Test runner to cover INSERT operations defined above.
   * @param name             Test name
   * @param initialSchemaDDL Initial schema of the table to be inserted into (as a DDL string).
   * @param initialJsonData  Initial data present in the table to be inserted into (as a JSON
   *                         string).
   * @param partitionBy      Partition columns for the initial table.
   * @param insertSchemaDDL  Schema of the data to be inserted (as a DDL string).
   * @param insertJsonData   Data to be inserted (as a JSON string)
   * @param overwriteWhere   Where clause for overwrite PARTITION / REPLACE WHERE (as
   *                         colName -> value)
   * @param expectedResult   Expected result, see [[ExpectedResult]] above.
   * @param includeInserts   List of insert types to run the test with. Defaults to all inserts.
   * @param excludeInserts   List of insert types to exclude when running the test. Defaults to no
   *                         inserts excluded.
   * @param confs            Custom spark confs to set before running the insert operation.
   */
  // scalastyle:off argcount
  def testInserts(name: String)(
      initialSchemaDDL: String,
      initialJsonData: Seq[String],
      partitionBy: Seq[String] = Seq.empty,
      insertSchemaDDL: String,
      insertJsonData: Seq[String],
      overwriteWhere: (String, Int),
      expectedResult: ExpectedResult,
      includeInserts: Seq[Insert] = allInsertTypes,
      excludeInserts: Seq[Insert] = Seq.empty,
      confs: Seq[(String, String)] = Seq.empty): Unit = {
    for (insert <- includeInserts.filterNot(excludeInserts.toSet)) {
      test(s"${insert.name} - $name") {
        withTable("source", "target") {
          val initialDF = readFromJSON(initialJsonData, StructType.fromDDL(initialSchemaDDL))
          val writer = initialDF.write.format("delta")
          if (partitionBy.nonEmpty) {
            writer.partitionBy(partitionBy: _*)
          }
          writer.saveAsTable("target")
          // Write the data to insert to a table so that we can use it in both SQL and dataframe
          // writer inserts.
          val insertDF = readFromJSON(insertJsonData, StructType.fromDDL(insertSchemaDDL))
          insertDF.write.format("delta").saveAsTable("source")

          def runInsert(): Unit =
            insert.runInsert(
              columns = insertDF.schema.map(_.name),
              whereCol = overwriteWhere._1,
              whereValue = overwriteWhere._2
            )

          withSQLConf(confs: _*) {
            expectedResult match {
              case Left(expectedSchema) =>
                runInsert()
                val target = spark.read.table("target")
                assert(target.schema === expectedSchema)
                checkAnswer(target, insert.expectedResult(initialDF, insertDF))
              case Right(checkError) =>
                val ex = intercept[SparkThrowable] {
                  runInsert()
                }
                checkError(ex)
            }
          }
        }
      }
    }
  }
  // scalastyle:on argcount
}
