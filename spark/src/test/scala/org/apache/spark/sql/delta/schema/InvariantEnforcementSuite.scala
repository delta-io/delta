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

package org.apache.spark.sql.delta.schema

// scalastyle:off import.ordering.noEmptyLine
import java.io.File
import java.sql.Date

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{CheckConstraintsTableFeature, DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.{Metadata, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints, Invariants}
import org.apache.spark.sql.delta.constraints.Constraints.NotNull
import org.apache.spark.sql.delta.constraints.Invariants.PersistedExpression
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class InvariantEnforcementSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils {


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
      assert(error.contains(expected), s"$error didn't contain $expected")
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

  test("reject non-nullable top level column") {
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

  test("reject non-nullable top level column - column doesn't exist") {
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
  }

  test("allow nullable struct with non-nullable nested column") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType, nullable = false)
        .add("value", IntegerType))
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(null)).asJava, schema.asNullable)
        .write.mode("append").format("delta").save(path)
    }
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

  testQuietly("reject non-nullable array element") {
    val schema = new StructType()
      .add("top", ArrayType(IntegerType, false))
    testBatchWriteRejection(
      NotNull(Seq("top", "element")),
      schema,
      spark.createDataFrame(Seq(Row(Seq(1, null))).asJava, schema.asNullable)
    )
  }

  test("allow null array with non-nullable element") {
    val schema = new StructType()
      .add("top", ArrayType(IntegerType, false))
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq(Row(null)).asJava, schema.asNullable).write
        .mode("append").format("delta").save(path)
    }
  }

  testQuietly("reject non-nullable array nested struct") {
    val schema = new StructType()
      .add("top", ArrayType(ArrayType(new StructType()
        .add("key", StringType)
        .add("value", IntegerType, nullable = false))))
    testBatchWriteRejection(
      NotNull(Seq("top", "element", "value")),
      schema,
      spark.createDataFrame(Seq(Row(Seq(Seq(Row("a", null))))).asJava, schema.asNullable)
    )

    val partialSchema = new StructType()
      .add("top", ArrayType(ArrayType(new StructType()
        .add("key", StringType))))

    testBatchWriteRejection(
      NotNull(Seq("top", "element", "value")),
      schema,
      spark.createDataFrame(Seq(Row(Seq(Seq(Row("a"))))).asJava, partialSchema)
    )
  }

  test("allow null array element with non-nullable nested field") {
    val schema = new StructType()
      .add("top", ArrayType(new StructType()
        .add("key", StringType)
        .add("value", IntegerType, nullable = false)))
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq(Row(Seq(null))).asJava, schema.asNullable).write
        .mode("append").format("delta").save(path)
    }
  }

  testQuietly("reject deeply nested array") {
    val schema = new StructType()
      .add("top", ArrayType(new StructType()
        .add("middle", ArrayType(new StructType()
          .add("child_arr", new ArrayType(IntegerType, false))
          .add("child_val", IntegerType, nullable = false)))))
    testBatchWriteRejection(
      NotNull(Seq("top", "element", "middle", "element", "child_arr", "element")),
      schema,
      spark.createDataFrame(Seq(Row(Seq(Row(Seq(Row(Seq(null), 1)))))).asJava, schema.asNullable)
    )

    testBatchWriteRejection(
      NotNull(Seq("top", "element", "middle", "element", "child_val")),
      schema,
      spark.createDataFrame(Seq(Row(Seq(Row(Seq(Row(Seq(1), null)))))).asJava, schema.asNullable)
    )
  }

  test("allow deeply nested array") {
    val schema = new StructType()
      .add("top", ArrayType(new StructType()
        .add("middle", ArrayType(new StructType()
          .add("child_arr", new ArrayType(IntegerType, false))
          .add("child_val", IntegerType, nullable = false)))))
    tableWithSchema(schema) { path =>
      val rows = Seq(
        Row(Seq(Row(Seq(Row(null, 1))))),
        Row(Seq(Row(Seq(null)))),
        Row(Seq(Row(null))),
        Row(Seq(null)),
        Row(null)
      )
      spark.createDataFrame(Seq(Row(Seq(null))).asJava, schema.asNullable).write
        .mode("append").format("delta").save(path)
    }
  }

  testQuietly("reject non-nullable map value") {
    val schema = new StructType()
      .add("top", MapType(IntegerType, IntegerType, false))
    testBatchWriteRejection(
      NotNull(Seq("top", "value")),
      schema,
      spark.createDataFrame(Seq(Row(Seq(1 -> null).toMap)).asJava, schema.asNullable)
    )
  }

  test("allow null map with non-nullable value") {
    val schema = new StructType()
      .add("top", MapType(IntegerType, IntegerType, false))
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq(Row(null)).asJava, schema.asNullable).write
        .mode("append").format("delta").save(path)
    }
  }

  testQuietly("reject non-nullable map key nested struct") {
    val schema = new StructType()
      .add("top", MapType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType, nullable = false),
        IntegerType, true))
    testBatchWriteRejection(
      NotNull(Seq("top", "key", "b")),
      schema,
      spark.createDataFrame(Seq(Row(Seq(Row(1, null) -> 2).toMap)).asJava, schema.asNullable)
    )
  }

  test("reject expression invariant on top level column") {
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

  for (writerVersion <- Seq(2, TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION))
  testQuietly("CHECK constraint is enforced if somehow created (writerVersion = " +
    s"$writerVersion)") {
    withSQLConf((DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key, writerVersion.toString)) {
      withTable("constraint") {
        spark.range(10).selectExpr("id AS valueA", "id AS valueB", "id AS valueC")
          .write.format("delta").saveAsTable("constraint")
        val table = DeltaTableV2(spark, TableIdentifier("constraint", None))
        val txn = table.startTransactionWithInitialSnapshot()
        val newMetadata = txn.metadata.copy(
          configuration = txn.metadata.configuration +
            ("delta.constraints.mychk" -> "valueA < valueB"))
        txn.commit(Seq(newMetadata), DeltaOperations.ManualUpdate)
        val protocol = table.deltaLog.update().protocol
        assert(protocol.implicitlyAndExplicitlySupportedFeatures
          .contains(CheckConstraintsTableFeature))
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
        val table = DeltaTableV2(spark, TableIdentifier("constraint", None))
        val txn = table.startTransactionWithInitialSnapshot()
        val newMetadata = txn.metadata.copy(
          configuration = txn.metadata.configuration +
            ("delta.constraints.mychk" -> "valueA < valueB"))
        txn.commit(Seq(newMetadata), DeltaOperations.ManualUpdate)
        spark.sql("ALTER TABLE constraint ADD COLUMN valueC INT")
      }
    }
  }

  // Helper function to construct the full test name as "RuntimeRepalceable: func"
  private def testReplaceableExpr(targetFunc: String, testTags: org.scalatest.Tag*)
    (testFun: => Any)
    (implicit pos: org.scalactic.source.Position): Unit = {
    val fulLTestName = s"RuntimeReplaceable: ${targetFunc}"
    // Suppress exceptions output for invariant violations
    super.test(fulLTestName) {
      testFun
    }
  }

  private def testReplaceable[T: Encoder](
    exprStr: String,
    colType: DataType,
    badValue: T) = {
    val rule = Constraints.Check("", spark.sessionState.sqlParser.parseExpression(exprStr))
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(exprStr).json)
      .build()
    val schema = new StructType()
      .add("value", colType, nullable = true, metadata)
    val rows = Seq(Row(badValue))
    testBatchWriteRejection(
      rule,
      schema,
      spark.createDataFrame(rows.toList.asJava, schema),
      "violated by row with values"
    )
    testStreamingWriteRejection[T](
      rule,
      schema,
      _.toDF().toDF("value"),
      Seq[T](badValue),
      "violated by row with values"
    )
  }

  testReplaceableExpr("assert_true") {
    testReplaceable("assert_true(value < 2) is not null", IntegerType, 1)
  }

  testReplaceableExpr("date_part") {
    testReplaceable("date_part('YEAR', value) < 2000", DateType, Date.valueOf("2001-01-01"))
  }

  testReplaceableExpr("decode") {
    testReplaceable("decode(encode(value, 'utf-8'), 'utf-8') = 'abc'", StringType, "a")
  }

  testReplaceableExpr("extract") {
    testReplaceable("extract(YEAR FROM value) < 2000", DateType, Date.valueOf("2001-01-01"))
  }

  testReplaceableExpr("ifnull") {
    testReplaceable("ifnull(value, 1) = 1", IntegerType, 2)
  }

  testReplaceableExpr("left") {
    testReplaceable("left(value, 1) = 'a'", StringType, "b")
  }

  testReplaceableExpr("right") {
    testReplaceable("right(value, 1) = 'a'", StringType, "b")
  }

  testReplaceableExpr("nullif") {
    testReplaceable("nullif(value, 1) = 2", IntegerType, 1)
  }

  testReplaceableExpr("nvl") {
    testReplaceable("nvl(value, 1) = 1", IntegerType, 2)
  }

  testReplaceableExpr("nvl2") {
    testReplaceable("nvl2(value, 1, 2) = 3", IntegerType, 2)
  }

  testReplaceableExpr("to_date") {
    testReplaceable("to_date(value) = '2001-01-01'", StringType, "2002-01-01")
  }

  testReplaceableExpr("to_timestamp") {
    testReplaceable(
      "to_timestamp(value) = '2001-01-01'",
      StringType,
      "2002-01-01 00:12:00")
  }


  // Helper function to test with empty to null conf on and off.
  private def testEmptyToNull(name: String)(f: => Any): Unit = {
    // Suppress exceptions output for invariant violations
    testQuietly(name) {
      Seq(true, false).foreach { enabled =>
        withSQLConf(
          DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL.key -> enabled.toString) {
          if (enabled) {
            f
          } else {
            intercept[Exception](f)
          }
        }
      }
    }
  }

  testEmptyToNull("reject empty string for NOT NULL string partition column - create") {
    val tblName = "empty_string_test"
    withTable(tblName) {
      sql(
        s"""
           |CREATE TABLE $tblName (
           |  c1 INT,
           |  c2 STRING NOT NULL
           |) USING delta
           |PARTITIONED BY (c2)
           |""".stripMargin)
      val ex = intercept[InvariantViolationException] (
        sql(
          s"""
             |INSERT INTO $tblName values (1, '')
             |""".stripMargin)
      )
      assert(ex.getMessage.contains("violated"))
    }
  }

  testEmptyToNull("reject empty string for NOT NULL string partition column - multiple") {
    val tblName = "empty_string_test"
    withTable(tblName) {
      sql(
        s"""
           |CREATE TABLE $tblName (
           |  c1 INT,
           |  c2 STRING NOT NULL,
           |  c3 STRING
           |) USING delta
           |PARTITIONED BY (c2, c3)
           |""".stripMargin)
      val ex = intercept[InvariantViolationException] (
        sql(
          s"""
             |INSERT INTO $tblName values (1, '', 'a')
             |""".stripMargin)
      )
      assert(ex.getMessage.contains("violated"))
      sql(
        s"""
           |INSERT INTO $tblName values (1, 'a', '')
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT COUNT(*) from $tblName where c3 IS NULL"),
        Row(1L)
      )
    }
  }

  testEmptyToNull("reject empty string for NOT NULL string partition column - multiple not null") {
    val tblName = "empty_string_test"
    withTable(tblName) {
      sql(
        s"""
           |CREATE TABLE $tblName (
           |  c1 INT,
           |  c2 STRING NOT NULL,
           |  c3 STRING NOT NULL
           |) USING delta
           |PARTITIONED BY (c2, c3)
           |""".stripMargin)
      val ex1 = intercept[InvariantViolationException] (
        sql(
          s"""
             |INSERT INTO $tblName values (1, '', 'a')
             |""".stripMargin)
      )
      assert(ex1.getMessage.contains("violated"))
      val ex2 = intercept[InvariantViolationException] (
        sql(
          s"""
             |INSERT INTO $tblName values (1, 'a', '')
             |""".stripMargin)
      )
      assert(ex2.getMessage.contains("violated"))
      val ex3 = intercept[InvariantViolationException] (
        sql(
          s"""
             |INSERT INTO $tblName values (1, '', '')
             |""".stripMargin)
      )
      assert(ex3.getMessage.contains("violated"))
    }
  }


  testEmptyToNull("reject empty string in check constraint") {
    val tblName = "empty_string_test"
    withTable(tblName) {
      sql(
        s"""
           |CREATE TABLE $tblName (
           |  c1 INT,
           |  c2 STRING
           |) USING delta
           |PARTITIONED BY (c2);
           |""".stripMargin)
      sql(
        s"""
           |ALTER TABLE $tblName ADD CONSTRAINT test CHECK (c2 IS NOT NULL)
           |""".stripMargin)
      intercept[InvariantViolationException] (
        sql(
          s"""
             |INSERT INTO ${tblName} VALUES (1, "")
             |""".stripMargin)
      )
    }
  }

  test("streaming with additional project") {
    withSQLConf(DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL.key -> "true") {
      val tblName = "test"
      withTable(tblName) {
        withTempDir { checkpointDir =>
          sql(
            s"""
               |CREATE TABLE $tblName (
               |  c1 INT,
               |  c2 STRING
               |) USING delta
               |PARTITIONED BY (c2);
               |""".stripMargin)
          sql(
            s"""
               |ALTER TABLE $tblName ADD CONSTRAINT cons CHECK (c1 > 0)
               |""".stripMargin)
          val path = DeltaLog.forTable(spark, TableIdentifier(tblName)).dataPath.toString
          val stream = MemoryStream[Int]
          val q = stream.toDF()
            .map(_ => Tuple2(1, "a"))
            .toDF("c1", "c2")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("delta")
            .start(path)
          stream.addData(1)
          q.processAllAvailable()
          q.stop()
        }
      }
    }
  }
}
