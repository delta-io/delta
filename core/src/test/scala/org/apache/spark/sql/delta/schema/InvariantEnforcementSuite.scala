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

package org.apache.spark.sql.delta.schema

// scalastyle:off import.ordering.noEmptyLine
import java.io.File

import scala.collection.JavaConverters._

 // Edge
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints, Invariants}
import org.apache.spark.sql.delta.constraints.Constraints.NotNull
import org.apache.spark.sql.delta.constraints.Invariants.PersistedExpression
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._

class InvariantEnforcementSuite extends QueryTest
    with SharedSparkSession    with DeltaSQLCommandTest
    with SQLTestUtils {


  import testImplicits._

  private def tableWithSchema(schema: StructType)(f: String => Unit): Unit = {
    withTempDir { tempDir =>
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val txn = deltaLog.startTransaction()
      txn.commit(Metadata(schemaString = schema.json) :: Nil, DeltaOperations.ManualUpdate)
      spark.read.format("delta")
        .load(tempDir.getAbsolutePath)
        .write
        .format("delta")
        .mode("overwrite")
        .save(tempDir.getAbsolutePath)
      f(tempDir.getAbsolutePath)
    }
  }

  private def testBatchWriteRejection(
      invariant: Constraint,
      schema: StructType,
      df: Dataset[_],
      expectedErrors: String*): Unit = {
    tableWithSchema(schema) { path =>
      val e = intercept[InvariantViolationException] {
        df.write.mode("append").format("delta").save(path)
      }
      checkConstraintException(e, (invariant.name +: expectedErrors): _*)
    }
  }

  private def checkConstraintException(
      e: InvariantViolationException, expectedErrors: String*): Unit = {
    val error = e.getMessage
    val allExpected = expectedErrors
    allExpected.foreach { expected =>
      assert(error.replaceAll("`", "").contains(expected.replaceAll("`", "")),
        s"$error didn't contain $expected")
    }
  }

  private def testStreamingWriteRejection[T: Encoder](
      invariant: Constraint,
      schema: StructType,
      toDF: MemoryStream[T] => DataFrame,
      data: Seq[T],
      expectedErrors: String*): Unit = {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      val txn = deltaLog.startTransaction()
      txn.commit(Metadata(schemaString = schema.json) :: Nil, DeltaOperations.ManualUpdate)
      val memStream = MemoryStream[T]
      val stream = toDF(memStream).writeStream
        .outputMode("append")
        .format("delta")
        .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
        .start(dir.getAbsolutePath)
      try {
        val e = intercept[StreamingQueryException] {
          memStream.addData(data)
          stream.processAllAvailable()
        }
        // Produce a good error if the cause isn't the right type - just an assert makes it hard to
        // see what the wrong exception was.
        intercept[InvariantViolationException] { throw e.getCause }

        checkConstraintException(
          e.getCause.asInstanceOf[InvariantViolationException],
          (invariant.name +: expectedErrors): _*)
      } finally {
        stream.stop()
      }
    }
  }

  testQuietly("reject non-nullable top level column") {
    val schema = new StructType()
      .add("key", StringType, nullable = false)
      .add("value", IntegerType)
    testBatchWriteRejection(
      NotNull(Seq("key")),
      schema,
      Seq[(String, Int)](("a", 1), (null, 2)).toDF("key", "value"),
      "key"
    )
    testStreamingWriteRejection[(String, Int)](
      NotNull(Seq("key")),
      schema,
      _.toDF().toDF("key", "value"),
      Seq[(String, Int)](("a", 1), (null, 2)),
      "key"
    )
  }

  testQuietly("reject non-nullable top level column - column doesn't exist") {
    val schema = new StructType()
      .add("key", StringType, nullable = false)
      .add("value", IntegerType)
    testBatchWriteRejection(
      NotNull(Seq("key")),
      schema,
      Seq[Int](1, 2).toDF("value"),
      "key"
    )
    testStreamingWriteRejection[Int](
      NotNull(Seq("key")),
      schema,
      _.toDF().toDF("value"),
      Seq[Int](1, 2),
      "key"
    )
  }

  testQuietly("write empty DataFrame - zero rows") {
    val schema = new StructType()
      .add("key", StringType, nullable = false)
      .add("value", IntegerType)
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq.empty[Row].asJava, schema.asNullable).write
        .mode("append").format("delta").save(path)
    }
  }

  testQuietly("write empty DataFrame - zero columns") {
    val schema = new StructType()
      .add("key", StringType, nullable = false)
      .add("value", IntegerType)
    testBatchWriteRejection(
      NotNull(Seq("key")),
      schema,
      Seq[Int](1, 2).toDF("value").drop("value"),
      "key"
    )
    testStreamingWriteRejection[Int](
      NotNull(Seq("key")),
      schema,
      _.toDF().toDF("value").drop("value"),
      Seq[Int](1, 2),
      "key"
    )
  }

  testQuietly("reject non-nullable nested column") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType, nullable = false)
        .add("value", IntegerType))
    testBatchWriteRejection(
      NotNull(Seq("key")),
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row(null, 2))).asJava, schema.asNullable),
      "top.key"
    )
    testBatchWriteRejection(
      NotNull(Seq("key")),
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(null)).asJava, schema.asNullable),
      "top.key"
    )
  }

  testQuietly("reject non-nullable array column") {
    val schema = new StructType()
      .add("top", ArrayType(ArrayType(new StructType()
        .add("key", StringType)
        .add("value", IntegerType))), nullable = false)
    testBatchWriteRejection(
      NotNull(Seq("top", "value")),
      schema,
      spark.createDataFrame(Seq(Row(Seq(Seq(Row("a", 1)))), Row(null)).asJava, schema.asNullable),
      "top"
    )
  }

  testQuietly("reject expression invariant on top level column") {
    val expr = "value < 3"
    val rule = Constraints.Check("", spark.sessionState.sqlParser.parseExpression(expr))
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true, metadata)
    testBatchWriteRejection(
      rule,
      schema,
      Seq[(String, Int)](("a", 1), (null, 5)).toDF("key", "value"),
      "value", "5"
    )
    testStreamingWriteRejection[(String, Int)](
      rule,
      schema,
      _.toDF().toDF("key", "value"),
      Seq[(String, Int)](("a", 1), (null, 5)),
      "value"
    )
  }

  testQuietly("reject expression invariant on nested column") {
    val expr = "top.value < 3"
    val rule = Constraints.Check("", spark.sessionState.sqlParser.parseExpression(expr))
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType)
        .add("value", IntegerType, nullable = true, metadata))
    testBatchWriteRejection(
      rule,
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row(null, 5))).asJava, schema.asNullable),
      "top.value", "5"
    )
  }

  testQuietly("reject write on top level expression invariant when field is null") {
    val expr = "value < 3"
    val rule = Constraints.Check("", spark.sessionState.sqlParser.parseExpression(expr))
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true, metadata)
    testBatchWriteRejection(
      rule,
      schema,
      Seq[String]("a", "b").toDF("key"),
      " - value : null"
    )
    testBatchWriteRejection(
      rule,
      schema,
      Seq[(String, Integer)](("a", 1), ("b", null)).toDF("key", "value"),
      " - value : null"
    )
  }

  testQuietly("reject write on nested expression invariant when field is null") {
    val expr = "top.value < 3"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val rule = Constraints.Check("", spark.sessionState.sqlParser.parseExpression(expr))
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType)
        .add("value", IntegerType, nullable = true, metadata))
    testBatchWriteRejection(
      rule,
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row("b", null))).asJava, schema.asNullable),
      " - top.value : null"
    )
    val schema2 = new StructType()
      .add("top", new StructType()
        .add("key", StringType))
    testBatchWriteRejection(
      rule,
      schema,
      spark.createDataFrame(Seq(Row(Row("a")), Row(Row("b"))).asJava, schema2.asNullable),
      " - top.value : null"
    )
  }

  testQuietly("is null on top level expression invariant when field is null") {
    val expr = "value is null or value < 3"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true, metadata)
    tableWithSchema(schema) { path =>
      Seq[String]("a", "b").toDF("key").write
        .mode("append").format("delta").save(path)
      Seq[(String, Integer)](("a", 1), ("b", null)).toDF("key", "value").write
        .mode("append").format("delta").save(path)
    }
  }

  testQuietly("is null on nested expression invariant when field is null") {
    val expr = "top.value is null or top.value < 3"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType)
        .add("value", IntegerType, nullable = true, metadata))
    val schema2 = new StructType()
      .add("top", new StructType()
        .add("key", StringType))
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row("b", null))).asJava, schema.asNullable)
        .write.mode("append").format("delta").save(path)
      spark.createDataFrame(Seq(Row(Row("a")), Row(Row("b"))).asJava, schema2.asNullable)
        .write.mode("append").format("delta").save(path)
    }
  }

  testQuietly("complex expressions - AND") {
    val expr = "value < 3 AND value > 0"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true, metadata)
    tableWithSchema(schema) { path =>
      Seq(1, 2).toDF("value").write.mode("append").format("delta").save(path)
      intercept[InvariantViolationException] {
        Seq(1, 4).toDF("value").write.mode("append").format("delta").save(path)
      }
      intercept[InvariantViolationException] {
        Seq(-1, 2).toDF("value").write.mode("append").format("delta").save(path)
      }
    }
  }

  testQuietly("complex expressions - IN SET") {
    val expr = "key in ('a', 'b', 'c')"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("key", StringType, nullable = true, metadata)
      .add("value", IntegerType)
    tableWithSchema(schema) { tempDir =>
      Seq("a", "b").toDF("key").write.mode("append").format("delta").save(tempDir)
      intercept[InvariantViolationException] {
        Seq("a", "d").toDF("key").write.mode("append").format("delta").save(tempDir)
      }
      intercept[InvariantViolationException] {
        Seq("e").toDF("key").write.mode("append").format("delta").save(tempDir)
      }
    }
  }

  test("CHECK constraint can't be created through SET TBLPROPERTIES") {
    withTable("noCheckConstraints") {
      spark.range(10).write.format("delta").saveAsTable("noCheckConstraints")
      val ex = intercept[AnalysisException] {
        spark.sql(
          "ALTER TABLE noCheckConstraints SET TBLPROPERTIES ('delta.constraints.mychk' = '1')")
      }
      assert(ex.getMessage.contains("ALTER TABLE ADD CONSTRAINT"))
    }
  }

  testQuietly("CHECK constraint is enforced if somehow created") {
    withSQLConf((DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key, "2")) {
      withTable("constraint") {
        spark.range(10).selectExpr("id AS valueA", "id AS valueB", "id AS valueC")
          .write.format("delta").saveAsTable("constraint")
        val log = DeltaLog.forTable(spark, TableIdentifier("constraint", None))
        val txn = log.startTransaction()
        val newMetadata = txn.metadata.copy(
          configuration = txn.metadata.configuration +
            ("delta.constraints.mychk" -> "valueA < valueB"))
        assert(txn.protocol.minWriterVersion === 2)
        txn.commit(Seq(newMetadata), DeltaOperations.ManualUpdate)
        assert(log.snapshot.protocol.minWriterVersion === 3)
        spark.sql("INSERT INTO constraint VALUES (50, 100, null)")
        val e = intercept[InvariantViolationException] {
          spark.sql("INSERT INTO constraint VALUES (100, 50, null)")
        }
        checkConstraintException(e,
          s"""CHECK constraint mychk (valueA < valueB) violated by row with values:
             | - valueA : 100
             | - valueB : 50""".stripMargin)

        val e2 = intercept[InvariantViolationException] {
          spark.sql("INSERT INTO constraint VALUES (100, null, null)")
        }
        checkConstraintException(e2,
          s"""CHECK constraint mychk (valueA < valueB) violated by row with values:
             | - valueA : 100
             | - valueB : null""".stripMargin)
      }
    }
  }

  test("table with CHECK constraint accepts other metadata changes") {
    withSQLConf((DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key, "3")) {
      withTable("constraint") {
        spark.range(10).selectExpr("id AS valueA", "id AS valueB")
          .write.format("delta").saveAsTable("constraint")
        val log = DeltaLog.forTable(spark, TableIdentifier("constraint", None))
        val txn = log.startTransaction()
        val newMetadata = txn.metadata.copy(
          configuration = txn.metadata.configuration +
            ("delta.constraints.mychk" -> "valueA < valueB"))
        txn.commit(Seq(newMetadata), DeltaOperations.ManualUpdate)
        spark.sql("ALTER TABLE constraint ADD COLUMN valueC INT")
      }
    }
  }

  def testUnenforcedNestedConstraints(
      testName: String,
      schemaString: String,
      expectedError: String,
      data: Row): Unit = {
    testQuietly(testName) {
      val nullTable = "nullTbl"
      withTable(nullTable) {
        // Try creating the table with the check enabled first, which should fail, then create it
        // for real with the check off which should succeed.
        if (expectedError != null) {
          val ex = intercept[AnalysisException] {
            sql(s"CREATE TABLE $nullTable ($schemaString) USING delta")
          }
          assert(ex.getMessage.contains(expectedError))
        }
        withSQLConf(("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")) {
          sql(s"CREATE TABLE $nullTable ($schemaString) USING delta")
        }

        // Once we've created the table, writes should succeed even if they violate the constraint.
        spark.createDataFrame(
          Seq(data).asJava,
          spark.table(nullTable).schema
        ).write.mode("append").format("delta").saveAsTable(nullTable)

        if (expectedError != null) {
          val ex = intercept[AnalysisException] {
            sql(s"REPLACE TABLE $nullTable ($schemaString) USING delta")
          }
          assert(ex.getMessage.contains(expectedError))
        }
        withSQLConf(("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")) {
          sql(s"REPLACE TABLE $nullTable ($schemaString) USING delta")
        }
      }
    }
  }

  testUnenforcedNestedConstraints(
    "not null within array",
    schemaString = "arr array<struct<name:string,mailbox:string NOT NULL>> NOT NULL",
    expectedError = "The element type of the field arr contains a NOT NULL constraint.",
    data = Row(Seq(Row("myName", null))))

  testUnenforcedNestedConstraints(
    "not null within map key",
    schemaString = "m map<struct<name:string,mailbox:string NOT NULL>, int> NOT NULL",
    expectedError = "The key type of the field m contains a NOT NULL constraint.",
    data = Row(Map(Row("myName", null) -> 1)))

  testUnenforcedNestedConstraints(
    "not null within map value",
    schemaString = "m map<int, struct<name:string,mailbox:string NOT NULL>> NOT NULL",
    expectedError = "The value type of the field m contains a NOT NULL constraint.",
    data = Row(Map(1 -> Row("myName", null))))

  testUnenforcedNestedConstraints(
    "not null within nested array",
    schemaString =
      "s struct<n:int NOT NULL, arr:array<struct<name:string,mailbox:string NOT NULL>> NOT NULL>",
    expectedError = "The element type of the field s.arr contains a NOT NULL constraint.",
    data = Row(Row(1, Seq(Row("myName", null)))))

}
