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

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.schema.Invariants.{ArbitraryExpression, NotNull, PersistedExpression}

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._

class InvariantEnforcementSuite extends QueryTest
    with SharedSparkSession    with SQLTestUtils {

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
      invariant: Invariants.Rule,
      schema: StructType,
      df: Dataset[_],
      expectedErrors: String*): Unit = {
    tableWithSchema(schema) { path =>
      val e = intercept[SparkException] {
        df.write.mode("append").format("delta").save(path)
      }
      var violationException = e.getCause
      while (violationException != null &&
             !violationException.isInstanceOf[InvariantViolationException]) {
        violationException = violationException.getCause
      }
      if (violationException == null) {
        fail("Didn't receive a InvariantViolationException.")
      }
      assert(violationException.isInstanceOf[InvariantViolationException])
      val error = violationException.getMessage
      val allExpected = Seq(invariant.name) ++ expectedErrors
      allExpected.foreach { expected =>
        assert(error.contains(expected), s"$error didn't contain $expected")
      }
    }
  }

  private def testStreamingWriteRejection[T: Encoder](
      invariant: Invariants.Rule,
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
        var violationException = e.getCause
        while (violationException != null &&
          !violationException.isInstanceOf[InvariantViolationException]) {
          violationException = violationException.getCause
        }
        if (violationException == null) {
          fail("Didn't receive a InvariantViolationException.")
        }
        assert(violationException.isInstanceOf[InvariantViolationException])
        val error = violationException.getMessage
        assert((Seq(invariant.name) ++ expectedErrors).forall(error.contains))
      } finally {
        stream.stop()
      }
    }
  }

  private def testStreamingWrite[T: Encoder](
      schema: StructType,
      toDF: MemoryStream[T] => DataFrame,
      data: Seq[T],
      expected: DataFrame): Unit = {
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
        memStream.addData(data)
        stream.processAllAvailable()

        checkAnswer(
          spark.read.format("delta").load(dir.getAbsolutePath),
          expected
        )
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
      NotNull,
      schema,
      Seq[(String, Int)](("a", 1), (null, 2)).toDF("key", "value"),
      "key"
    )
    testStreamingWriteRejection[(String, Int)](
      NotNull,
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
      NotNull,
      schema,
      Seq[Int](1, 2).toDF("value"),
      "key"
    )
    testStreamingWriteRejection[Int](
      NotNull,
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
      NotNull,
      schema,
      Seq[Int](1, 2).toDF("value").drop("value"),
      "key"
    )
    testStreamingWriteRejection[Int](
      NotNull,
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
      NotNull,
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row(null, 2))).asJava, schema.asNullable),
      "top.key"
    )
    testBatchWriteRejection(
      NotNull,
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(null)).asJava, schema.asNullable),
      "top.key"
    )
  }

  testQuietly("complex type - children of array type can't be checked") {
    val schema = new StructType()
      .add("top", ArrayType(ArrayType(new StructType()
        .add("key", StringType, nullable = false)
        .add("value", IntegerType))))
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq(Row(Seq(Seq(Row("a", 1)))), Row(Seq(Seq(Row(null, 2))))).asJava,
        schema.asNullable).write.mode("append").format("delta").save(path)
      spark.createDataFrame(Seq(Row(Seq(Seq(Row("a", 1)))), Row(null)).asJava, schema.asNullable)
        .write.mode("append").format("delta").save(path)
    }
  }

  testQuietly("reject non-nullable array column") {
    val schema = new StructType()
      .add("top", ArrayType(ArrayType(new StructType()
        .add("key", StringType)
        .add("value", IntegerType))), nullable = false)
    testBatchWriteRejection(
      NotNull,
      schema,
      spark.createDataFrame(Seq(Row(Seq(Seq(Row("a", 1)))), Row(null)).asJava, schema.asNullable),
      "top"
    )
  }

  testQuietly("reject expression invariant on top level column") {
    val expr = "value < 3"
    val rule = ArbitraryExpression(spark, expr)
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
    val expr = "top.key < 3"
    val rule = ArbitraryExpression(spark, expr)
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
      "top.key", "5"
    )
  }

  testQuietly("reject write on top level expression invariant when field is null") {
    val expr = "value < 3"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val rule = ArbitraryExpression(spark, expr)
    val schema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true, metadata)
    testBatchWriteRejection(
      rule,
      schema,
      Seq[String]("a", "b").toDF("key"),
      "value", "null"
    )
    testBatchWriteRejection(
      rule,
      schema,
      Seq[(String, Integer)](("a", 1), ("b", null)).toDF("key", "value"),
      "value", "null"
    )
  }

  testQuietly("reject write on nested expression invariant when field is null") {
    val expr = "top.value < 3"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val rule = ArbitraryExpression(spark, expr)
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType)
        .add("value", IntegerType, nullable = true, metadata))
    testBatchWriteRejection(
      rule,
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row("b", null))).asJava, schema.asNullable),
      "top.value", "null"
    )
    val schema2 = new StructType()
      .add("top", new StructType()
        .add("key", StringType))
    testBatchWriteRejection(
      rule,
      schema,
      spark.createDataFrame(Seq(Row(Row("a")), Row(Row("b"))).asJava, schema2.asNullable),
      "top.value", "null"
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
      intercept[SparkException] {
        Seq(1, 4).toDF("value").write.mode("append").format("delta").save(path)
      }
      intercept[SparkException] {
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
      intercept[SparkException] {
        Seq("a", "d").toDF("key").write.mode("append").format("delta").save(tempDir)
      }
      intercept[SparkException] {
        Seq("e").toDF("key").write.mode("append").format("delta").save(tempDir)
      }
    }
  }
}
