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

package org.apache.spark.sql.delta.typewidening

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.sources.{DeltaSink, DeltaSQLConf}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
 * Suite covering automatic type widening in the Delta streaming sink.
 */
class TypeWideningStreamingSinkSuite
  extends DeltaSinkImplicitCastSuiteBase
  with TypeWideningTestMixin {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Set by default confs to enable automatic type widening in all tests. Negative tests should
    // explicitly disable these.
    spark.conf.set(DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key, "true")
    spark.conf.set(DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey, "true")
    spark.conf.set(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")
    // Ensure we don't silently cast test inputs to null on overflow.
    spark.conf.set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("type isn't widened if schema evolution is disabled") {
    withDeltaStream[Int] { stream =>
      stream.write(17)("CAST(value AS SHORT)")
      assert(stream.currentSchema("value").dataType === ShortType)
      checkAnswer(stream.read(), Row(17))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "false") {
        stream.write(53)("CAST(value AS INT)")
        assert(stream.currentSchema("value").dataType === ShortType)
        checkAnswer(stream.read(), Row(17) :: Row(53) :: Nil)
      }
    }
  }

  test("type isn't widened if type widening is disabled") {
    withDeltaStream[Int] { stream =>
      withSQLConf(DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey -> "false") {
        stream.write(17)("CAST(value AS SHORT)")
        assert(stream.currentSchema("value").dataType === ShortType)
        checkAnswer(stream.read(), Row(17))

        stream.write(53)("CAST(value AS INT)")
        assert(stream.currentSchema("value").dataType === ShortType)
        checkAnswer(stream.read(), Row(17) :: Row(53) :: Nil)
      }
    }
  }

  test("type is widened if type widening and schema evolution are enabled") {
    withDeltaStream[Int] { stream =>
      stream.write(17)("CAST(value AS SHORT)")
      assert(stream.currentSchema("value").dataType === ShortType)
      checkAnswer(stream.read(), Row(17))

      stream.write(Int.MaxValue)("CAST(value AS INT)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17) :: Row(Int.MaxValue) :: Nil)
    }
  }

  test("type can be widened even if type casting is disabled in the sink") {
    withDeltaStream[Int] { stream =>
      stream.write(17)("CAST(value AS SHORT)")
      assert(stream.currentSchema("value").dataType === ShortType)
      checkAnswer(stream.read(), Row(17))

      withSQLConf(DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key -> "false") {
        stream.write(Int.MaxValue)("CAST(value AS INT)")
        assert(stream.currentSchema("value").dataType === IntegerType)
        checkAnswer(stream.read(), Row(17) :: Row(Int.MaxValue) :: Nil)
      }
    }
  }

  test("type isn't changed if it's not a wider type") {
    withDeltaStream[Int] { stream =>
      stream.write(Int.MaxValue)("CAST(value AS INT)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(Int.MaxValue))

      stream.write(17)("CAST(value AS SHORT)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(Int.MaxValue) :: Row(17) :: Nil)
    }
  }

  test("type isn't changed if it's not eligible for automatic widening: int -> decimal") {
    withDeltaStream[Int] { stream =>
      stream.write(17)("CAST(value AS INT)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17))

      stream.write(567)("CAST(value AS DECIMAL(20, 0))")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17) :: Row(567) :: Nil)
    }
  }

  test("type isn't changed if it's not eligible for automatic widening: int -> double") {
    withDeltaStream[Int] { stream =>
      stream.write(17)("CAST(value AS INT)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17))

      stream.write(567)("CAST(value AS DOUBLE)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17) :: Row(567) :: Nil)
    }
  }

  test("widen type and add a new column with schema evolution") {
    withDeltaStream[(Int, Int)] { stream =>
      stream.write((17, -1))("CAST(_1 AS SHORT) AS a")
      assert(stream.currentSchema === new StructType().add("a", ShortType))
      checkAnswer(stream.read(), Row(17))

      stream.write((12, 3456))("CAST(_1 AS INT) AS a", "CAST(_2 AS DECIMAL(10, 2)) AS b")
      assert(stream.currentSchema === new StructType()
        .add("a", IntegerType, nullable = true,
          metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))
        .add("b", DecimalType(10, 2)))
      checkAnswer(stream.read(), Row(17, null) :: Row(12, 3456) :: Nil)
    }
  }

  test("widen type during write with missing column") {
    withDeltaStream[(Int, Int)] { stream =>
      stream.write((17, 45))("CAST(_1 AS SHORT) AS a", "CAST(_2 AS LONG) AS b")
      assert(stream.currentSchema === new StructType()
        .add("a", ShortType)
        .add("b", LongType))
      checkAnswer(stream.read(), Row(17, 45))

      stream.write((12, -1))("CAST(_1 AS INT) AS a")
      assert(stream.currentSchema === new StructType()
        .add("a", IntegerType, nullable = true,
          metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))
        .add("b", LongType))
      checkAnswer(stream.read(), Row(17, 45) :: Row(12, null) :: Nil)
    }
  }

  test("widen type after column rename and drop") {
    withDeltaStream[(Int, Int)] { stream =>
      stream.write((17, 45))("CAST(_1 AS SHORT) AS a", "CAST(_2 AS DECIMAL(10, 0)) AS b")
      assert(stream.currentSchema === new StructType()
        .add("a", ShortType)
        .add("b", DecimalType(10, 0)))
      checkAnswer(stream.read(), Row(17, 45))

      sql(
        s"""
           |ALTER TABLE delta.`${stream.deltaLog.dataPath}` SET TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.minReaderVersion' = '2',
           |  'delta.minWriterVersion' = '5'
           |)
         """.stripMargin)
      sql(s"ALTER TABLE delta.`${stream.deltaLog.dataPath}` DROP COLUMN b")
      sql(s"ALTER TABLE delta.`${stream.deltaLog.dataPath}` RENAME COLUMN a to c")
      assert(stream.currentSchema === new StructType().add("c", ShortType))

      stream.write((12, -1))("CAST(_1 AS INT) AS c")
      assert(stream.currentSchema === new StructType().add("c", IntegerType, nullable = true,
          metadata = typeWideningMetadata(version = 4, from = ShortType, to = IntegerType)))
      checkAnswer(stream.read(), Row(17) :: Row(12) :: Nil)
    }
  }

  test("type widening in addBatch") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      sqlContext.sparkContext.setLocalProperty(StreamExecution.QUERY_ID_KEY, "streaming_query")
      val sink = DeltaSink(
        sqlContext,
        path = deltaLog.dataPath,
        partitionColumns = Seq.empty,
        outputMode = OutputMode.Append(),
        options = new DeltaOptions(options = Map.empty, conf = spark.sessionState.conf)
      )

      val schema = new StructType().add("value", ShortType)

      {
        val data = Seq(0, 1).toDF("value").selectExpr("CAST(value AS SHORT)")
        sink.addBatch(0, data)
        val df = spark.read.format("delta").load(tablePath)
        assert(df.schema === schema)
        checkAnswer(df, Row(0) :: Row(1) :: Nil)
      }
      {
        val data = Seq(2, 3).toDF("value").selectExpr("CAST(value AS INT)")
        sink.addBatch(1, data)
        val df = spark.read.format("delta").load(tablePath)
        assert(df.schema === new StructType().add("value", IntegerType, nullable = true,
          metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType)))
        checkAnswer(df, Row(0) :: Row(1) :: Row(2) :: Row(3) :: Nil)
      }
    }
  }
}
