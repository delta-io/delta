/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.util.concurrent.CountDownLatch

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

trait SchemaValidationSuiteBase extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  def checkMergeException(e: Exception, col: String): Unit = {
    assert(e.isInstanceOf[MetadataChangedException])
    assert(e.getMessage.contains(
      "The metadata of the Delta table has been changed by a concurrent update"))
  }
}

/**
 * This Suite tests the behavior of Delta commands when a schema altering commit is run after the
 * command completes analysis but before the command starts the transaction. We want to make sure
 * That we do not corrupt tables.
 */
class SchemaValidationSuite extends SchemaValidationSuiteBase {

  class BlockingRule(
      blockActionLatch: CountDownLatch,
      startConcurrentUpdateLatch: CountDownLatch) extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      startConcurrentUpdateLatch.countDown()
      blockActionLatch.await()
      plan
    }
  }

  /**
   * Blocks the thread with the help of an optimizer rule until end of scope.
   * We need two latches to ensure that the thread executing the query is blocked until
   * the other thread concurrently updates the metadata. `blockActionLatch` blocks the action
   * until it is counted down by the thread updating the metadata. `startConcurrentUpdateLatch`
   * will block the concurrent update to happen until it is counted down by the action reaches the
   * optimizer rule.
  */
  private def withBlockedExecution(
      t: Thread,
      blockActionLatch: CountDownLatch,
      startConcurrentUpdateLatch: CountDownLatch)(f: => Unit): Unit = {
    t.start()
    startConcurrentUpdateLatch.await()
    try {
      f
    } finally {
      blockActionLatch.countDown()
      t.join()
    }
  }

  def cloneSession(spark: SparkSession): SparkSession = {
    val cloneMethod = classOf[SparkSession].getDeclaredMethod("cloneSession")
    cloneMethod.setAccessible(true)
    val clonedSession = cloneMethod.invoke(spark).asInstanceOf[SparkSession]
    clonedSession
  }

  /**
   * Common base method for both the path based and table name based tests.
   */
  private def testConcurrentChangeBase(identifier: String)(
      createTable: (SparkSession, String) => Unit,
      actionToTest: (SparkSession, String) => Unit,
      concurrentChange: (SparkSession, String) => Unit): Unit = {
    createTable(spark, identifier)

    // Clone the session to run the query in a separate thread.
    val newSession = cloneSession(spark)
    val blockActionLatch = new CountDownLatch(1)
    val startConcurrentUpdateLatch = new CountDownLatch(1)
    val rule = new BlockingRule(blockActionLatch, startConcurrentUpdateLatch)
    newSession.experimental.extraOptimizations :+= rule

    var actionException: Exception = null
    val actionToTestThread = new Thread() {
      override def run(): Unit = {
        try {
          actionToTest(newSession, identifier)
        } catch {
          case e: Exception =>
            actionException = e
        }
      }
    }
    withBlockedExecution(actionToTestThread, blockActionLatch, startConcurrentUpdateLatch) {
      concurrentChange(spark, identifier)
    }
    if (actionException != null) {
      throw actionException
    }
 }

  /**
   * tests the behavior of concurrent changes to schema on a blocked command.
   * @param testName - name of the test
   * @param createTable - method that creates a table given an identifier and spark session.
   * @param actionToTest - the method we want to test.
   * @param concurrentChange - the concurrent query that updates the schema of the table
   *
   * All the above methods take SparkSession and the table path as parameters
   */
  def testConcurrentChange(testName: String)(
    createTable: (SparkSession, String) => Unit,
    actionToTest: (SparkSession, String) => Unit,
    concurrentChange: (SparkSession, String) => Unit): Unit = {

    test(testName) {
      withTempDir { tempDir =>
        testConcurrentChangeBase(tempDir.getCanonicalPath)(
          createTable,
          actionToTest,
          concurrentChange
        )
      }
    }
  }

  /**
   * tests the behavior of concurrent changes pf schema on a blocked command with metastore tables.
   * @param testName - name of the test
   * @param createTable - method that creates a table given an identifier and spark session.
   * @param actionToTest - the method we want to test.
   * @param concurrentChange - the concurrent query that updates the schema of the table
   *
   * All the above methods take SparkSession and the table name as parameters
   */
  def testConcurrentChangeWithTable(testName: String)(
    createTable: (SparkSession, String) => Unit,
    actionToTest: (SparkSession, String) => Unit,
    concurrentChange: (SparkSession, String) => Unit): Unit = {

    val tblName = "metastoreTable"
    test(testName) {
      withTable(tblName) {
        testConcurrentChangeBase(tblName)(
          createTable,
          actionToTest,
          concurrentChange
        )
      }
    }
  }

  /**
   * Creates a method to remove a column from the table by taking column as an argument.
   */
  def dropColFromSampleTable(col: String): (SparkSession, String) => Unit = {
    (spark: SparkSession, tblPath: String) => {
      spark.read.format("delta").load(tblPath)
        .drop(col)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(tblPath)
    }
  }

  /**
   * Adding a column to the schema will result in the blocked thread appending to the table
   * with null values for the new column.
   */
  testConcurrentChange("write - add a column concurrently")(
    createTable = (spark: SparkSession, tblPath: String) => {
      spark.range(10).write.format("delta").save(tblPath)
    },
    actionToTest = (spark: SparkSession, tblPath: String) => {
      spark.range(11, 20).write.format("delta")
        .mode("append")
        .save(tblPath)

      val appendedCol2Values = spark.read.format("delta")
        .load(tblPath)
        .filter(col("id") <= 20)
        .select("col2")
        .distinct()
        .collect()
        .toList
      assert(appendedCol2Values == List(Row(null)))
    },
    concurrentChange = (spark: SparkSession, tblPath: String) => {
      spark.range(21, 30).withColumn("col2", lit(2)).write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(tblPath)
    }
  )

  /**
   * Removing a column while a query is in running should throw an analysis
   * exception
   */
  testConcurrentChange("write - remove a column concurrently")(
    createTable = (spark: SparkSession, tblPath: String) => {
      spark.range(10).withColumn("col2", lit(1))
        .write
        .format("delta")
        .save(tblPath)
    },
    actionToTest = (spark: SparkSession, tblPath: String) => {
      val e = intercept[AnalysisException] {
        spark.range(11, 20)
          .withColumn("col2", lit(1)).write.format("delta")
          .mode("append")
          .save(tblPath)
      }
      assert(e.getMessage.contains(
        "A schema mismatch detected when writing to the Delta table"))
    },
    concurrentChange = dropColFromSampleTable("col2")
  )

  /**
   * Removing a column while performing a delete should be caught while
   * writing the deleted files(i.e files with rows that were not deleted).
   */
  testConcurrentChange("delete - remove a column concurrently")(
    createTable = (spark: SparkSession, tblPath: String) => {
      spark.range(10).withColumn("col2", lit(1))
        .write
        .format("delta")
        .save(tblPath)
    },
    actionToTest = (spark: SparkSession, tblPath: String) => {
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tblPath)
      val e = intercept[Exception] {
        deltaTable.delete(col("id") === 1)
      }
      assert(e.getMessage.contains(s"Can't resolve column col2"))
    },
    concurrentChange = dropColFromSampleTable("col2")
  )

  /**
   * Removing a column(referenced in condition) while performing a delete will
   * result in a no-op.
   */
  testConcurrentChange("delete - remove condition column concurrently")(
    createTable = (spark: SparkSession, tblPath: String) => {
      spark.range(10).withColumn("col2", lit(1))
        .repartition(2)
        .write
        .format("delta")
        .save(tblPath)
    },
    actionToTest = (spark: SparkSession, tblPath: String) => {
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tblPath)
      deltaTable.delete(col("id") === 1)
      // check if delete is no-op
      checkAnswer(
        deltaTable.history.select("operation"),
        Seq(Row("WRITE"), Row("WRITE")))
    },
    concurrentChange = dropColFromSampleTable("id")
  )

  /**
   * An update command that has to rewrite files will have the old schema,
   * we catch the outdated schema during the write.
   */
  testConcurrentChange("update - remove a column concurrently")(
    createTable = (spark: SparkSession, tblPath: String) => {
      spark.range(10).withColumn("col2", lit(1))
        .write
        .format("delta")
        .save(tblPath)
    },
    actionToTest = (spark: SparkSession, tblPath: String) => {
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tblPath)
      val e = intercept[AnalysisException] {
        deltaTable.update(col("id") =!= 1, Map("col2" -> lit(-1)))
      }
      assert(e.getMessage.contains(s"Can't resolve column col2"))
    },
    concurrentChange = dropColFromSampleTable("col2")
  )

  /**
   * Removing a column(referenced in condition) while performing a update will
   * result in a no-op.
   */
  testConcurrentChange("update - remove condition column concurrently")(
    createTable = (spark: SparkSession, tblPath: String) => {
      spark.range(10).withColumn("col2", lit(1))
        .repartition(2)
        .write
        .format("delta")
        .save(tblPath)
    },
    actionToTest = (spark: SparkSession, tblPath: String) => {
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tblPath)
      deltaTable.update(col("id") === 1, Map("id" -> lit("2")))
      // check if update is no-op
      checkAnswer(
        deltaTable.history.select("operation"),
        Seq(Row("WRITE"), Row("WRITE")))
    },
    concurrentChange = dropColFromSampleTable("id")
  )

  /**
   * Concurrently drop column in merge condition. Merge command detects the schema change while
   * resolving the target and throws an AnalysisException
   */
  testConcurrentChange("merge - remove a column in merge condition concurrently")(
    createTable = (spark: SparkSession, tblPath: String) => {
      spark.range(10).withColumn("col2", lit(1))
        .write
        .format("delta")
        .save(tblPath)
    },
    actionToTest = (spark: SparkSession, tblPath: String) => {
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tblPath)
      val sourceDf = spark.range(10).withColumn("col2", lit(2))
      val e = intercept[Exception] {
        deltaTable.as("t1")
          .merge(sourceDf.as("t2"), "t1.id == t2.id")
          .whenNotMatched()
          .insertAll()
          .whenMatched()
          .updateAll()
          .execute()
      }
      checkMergeException(e, "id")
    },
    concurrentChange = dropColFromSampleTable("id")
  )

  /**
   * Concurrently drop column not in merge condition but in target. Merge command detects the schema
   * change while resolving the target and throws an AnalysisException
   */
  testConcurrentChange("merge - remove a column not in merge condition concurrently")(
    createTable = (spark: SparkSession, tblPath: String) => {
      spark.range(10).withColumn("col2", lit(1))
        .write
        .format("delta")
        .save(tblPath)
    },
    actionToTest = (spark: SparkSession, tblPath: String) => {
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tblPath)
      val sourceDf = spark.range(10).withColumn("col2", lit(2))
      val e = intercept[Exception] {
        deltaTable.as("t1")
          .merge(sourceDf.as("t2"), "t1.id == t2.id")
          .whenNotMatched()
          .insertAll()
          .whenMatched()
          .updateAll()
          .execute()
      }
      checkMergeException(e, "col2")
    },
    concurrentChange = dropColFromSampleTable("col2")
  )

  /**
   * Alter table to add a column and at the same time add a column concurrently.
   */
  testConcurrentChangeWithTable("alter table add column - remove column and add same column")(
    createTable = (spark: SparkSession, tblName: String) => {
      spark.range(10).write.format("delta").saveAsTable(tblName)
    },
    actionToTest = (spark: SparkSession, tblName: String) => {
      val e = intercept[AnalysisException] {
        spark.sql(s"ALTER TABLE `$tblName` ADD COLUMNS (col2 string)")
      }
      assert(e.getMessage.contains("Found duplicate column(s) in adding columns: col2"))
    },
    concurrentChange = (spark: SparkSession, tblName: String) => {
      spark.read.format("delta").table(tblName)
        .withColumn("col2", lit(1))
        .write
        .format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .saveAsTable(tblName)
    }
  )
}

