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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._

sealed trait SaveOperation {
  def apply(dfw: DataFrameWriter[_]): Unit
}

case class SaveWithPath(path: String = null) extends SaveOperation {
  override def apply(dfw: DataFrameWriter[_]): Unit = {
    if (path == null) dfw.save() else dfw.save(path)
  }
}

case class SaveAsTable(tableName: String) extends SaveOperation {
  override def apply(dfw: DataFrameWriter[_]): Unit = dfw.saveAsTable(tableName)
}

sealed trait SchemaEnforcementSuiteBase
  extends QueryTest with SharedSparkSession {
  protected def enableAutoMigration(f: => Unit): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
      f
    }
  }

  protected def disableAutoMigration(f: => Unit): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "false") {
      f
    }
  }
}

sealed trait BatchWriterTest extends SchemaEnforcementSuiteBase with SharedSparkSession {

  def saveOperation: SaveOperation

  implicit class RichDataFrameWriter(dfw: DataFrameWriter[_]) {
    def append(path: File): Unit = {
      saveOperation(dfw.format("delta").mode("append").option("path", path.getAbsolutePath))
    }

    def overwrite(path: File): Unit = {
      saveOperation(dfw.format("delta").mode("overwrite").option("path", path.getAbsolutePath))
    }
  }

  def equivalenceTest(testName: String)(f: => Unit): Unit = {
    test(s"batch: $testName") {
      saveOperation match {
        case _: SaveWithPath => f
        case SaveAsTable(tbl) => withTable(tbl) { f }
      }
    }
  }
}

trait AppendSaveModeNullTests extends BatchWriterTest {
  import testImplicits._

  equivalenceTest("JSON ETL workflow, NullType being only data column") {
    enableAutoMigration {
      val row1 = """{"key":"abc","id":null}"""
      withTempDir { dir =>
        val schema1 = new StructType().add("key", StringType).add("id", NullType)
        val e = intercept[AnalysisException] {
          spark.read.schema(schema1).json(Seq(row1).toDS()).write.partitionBy("key").append(dir)
        }
        assert(e.getMessage.contains("NullType have been dropped"))
      }
    }
  }

  equivalenceTest("JSON ETL workflow, schema merging NullTypes") {
    enableAutoMigration {
      val row1 = """{"key":"abc","id":null,"extra":1}"""
      val row2 = """{"key":"def","id":2,"extra":null}"""
      val row3 = """{"key":"ghi","id":null,"extra":3}"""
      withTempDir { dir =>
        val schema1 = new StructType()
          .add("key", StringType).add("id", NullType).add("extra", IntegerType)
        val schema2 = new StructType()
          .add("key", StringType).add("id", IntegerType).add("extra", NullType)
        spark.read.schema(schema1).json(Seq(row1).toDS()).write.append(dir)

        // NullType will be removed during the read
        checkAnswer(
          spark.read.format("delta").load(dir.getAbsolutePath),
          Row("abc", 1) :: Nil
        )

        spark.read.schema(schema2).json(Seq(row2).toDS()).write.append(dir)
        spark.read.schema(schema1).json(Seq(row3).toDS()).write.append(dir)

        checkAnswer(
          spark.read.format("delta").load(dir.getAbsolutePath),
          Row("abc", null, 1) :: Row("def", 2, null) :: Row("ghi", null, 3) :: Nil
        )
      }
    }
  }

  equivalenceTest("JSON ETL workflow, schema merging NullTypes - nested struct") {
    enableAutoMigration {
      val row1 = """{"key":"abc","top":{"id":null,"extra":1}}"""
      val row2 = """{"key":"def","top":{"id":2,"extra":null}}"""
      val row3 = """{"key":"ghi","top":{"id":null,"extra":3}}"""
      withTempDir { dir =>
        val schema1 = new StructType().add("key", StringType)
          .add("top", new StructType().add("id", NullType).add("extra", IntegerType))
        val schema2 = new StructType().add("key", StringType)
          .add("top", new StructType().add("id", IntegerType).add("extra", NullType))
        val mergedSchema = new StructType().add("key", StringType)
          .add("top", new StructType().add("id", IntegerType).add("extra", IntegerType))
        spark.read.schema(schema1).json(Seq(row1).toDS()).write.append(dir)
        // NullType will be removed during the read
        checkAnswer(
          spark.read.format("delta").load(dir.getAbsolutePath),
          Row("abc", Row(1)) :: Nil
        )

        spark.read.schema(schema2).json(Seq(row2).toDS()).write.append(dir)
        assert(spark.read.format("delta").load(dir.getAbsolutePath).schema === mergedSchema)
        spark.read.schema(schema1).json(Seq(row3).toDS()).write.append(dir)
        assert(spark.read.format("delta").load(dir.getAbsolutePath).schema === mergedSchema)

        checkAnswer(
          spark.read.format("delta").load(dir.getAbsolutePath),
          Row("abc", Row(null, 1)) :: Row("def", Row(2, null)) :: Row("ghi", Row(null, 3)) :: Nil
        )
      }
    }
  }

  equivalenceTest("JSON ETL workflow, schema merging NullTypes - throw error on complex types") {
    enableAutoMigration {
      val row1 = """{"key":"abc","top":[]}"""
      val row2 = """{"key":"abc","top":[{"id":null}]}"""
      withTempDir { dir =>
        val schema1 = new StructType().add("key", StringType).add("top", ArrayType(NullType))
        val schema2 = new StructType().add("key", StringType)
          .add("top", ArrayType(new StructType().add("id", NullType)))
        val e1 = intercept[AnalysisException] {
          spark.read.schema(schema1).json(Seq(row1).toDS()).write.append(dir)
        }
        assert(e1.getMessage.contains("NullType"))
        val e2 = intercept[AnalysisException] {
          spark.read.schema(schema2).json(Seq(row2).toDS()).write.append(dir)
        }
        assert(e2.getMessage.contains("NullType"))
      }
    }
  }
}

trait AppendSaveModeTests extends BatchWriterTest {
  import testImplicits._

  equivalenceTest("reject schema changes by default") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(10).write.append(dir)
        val e = intercept[AnalysisException] {
          spark.range(10).withColumn("part", 'id + 1).write.append(dir)
        }
        assert(e.getMessage.contains(DeltaOptions.MERGE_SCHEMA_OPTION))
      }
    }
  }

  equivalenceTest("allow schema changes when autoMigrate is enabled") {
    enableAutoMigration {
      withTempDir { dir =>
        spark.range(10).write.append(dir)
        spark.range(10).withColumn("part", 'id + 1).write.append(dir)
        assert(spark.read.format("delta").load(dir.getAbsolutePath).schema.length == 2)
      }
    }
  }

  equivalenceTest("disallow schema changes when autoMigrate enabled but writer config disabled") {
    enableAutoMigration {
      withTempDir { dir =>
        spark.range(10).write.append(dir)
        val e = intercept[AnalysisException] {
          spark.range(10).withColumn("part", 'id + 1).write
            .option(DeltaOptions.MERGE_SCHEMA_OPTION, "false").append(dir)
        }
        assert(e.getMessage.contains(DeltaOptions.MERGE_SCHEMA_OPTION))
      }
    }
  }

  equivalenceTest("allow schema change with option") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(10).write.append(dir)
        spark.range(10).withColumn("part", 'id + 1).write
          .option(DeltaOptions.MERGE_SCHEMA_OPTION, "true").append(dir)
        assert(spark.read.format("delta").load(dir.getAbsolutePath).schema.length == 2)
      }
    }
  }

  equivalenceTest("JSON ETL workflow, NullType partition column should fail") {
    enableAutoMigration {
      val row1 = """{"key":"abc","id":null}"""
      withTempDir { dir =>
        val schema1 = new StructType().add("key", StringType).add("id", NullType)
        intercept[AnalysisException] {
          spark.read.schema(schema1).json(Seq(row1).toDS()).write.partitionBy("id").append(dir)
        }
        intercept[AnalysisException] {
          // check case sensitivity with regards to column dropping
          spark.read.schema(schema1).json(Seq(row1).toDS()).write.partitionBy("iD").append(dir)
        }
      }
    }
  }

  equivalenceTest("reject columns that only differ by case - append") {
    withTempDir { dir =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        intercept[AnalysisException] {
          spark.range(10).withColumn("ID", 'id + 1).write.append(dir)
        }

        intercept[AnalysisException] {
          spark.range(10).withColumn("ID", 'id + 1).write
            .option(DeltaOptions.MERGE_SCHEMA_OPTION, "true").append(dir)
        }

        intercept[AnalysisException] {
          spark.range(10).withColumn("a", 'id + 1).write
            .partitionBy("a", "A")
            .option(DeltaOptions.MERGE_SCHEMA_OPTION, "true").append(dir)
        }
      }
    }
  }

  equivalenceTest("ensure schema mismatch error message contains table ID") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(10).write.append(dir)
        val e = intercept[AnalysisException] {
          spark.range(10).withColumn("part", 'id + 1).write.append(dir)
        }
        assert(e.getMessage.contains("schema mismatch detected"))
        assert(e.getMessage.contains(s"Table ID: ${DeltaLog.forTable(spark, dir).tableId}"))
      }
    }
  }
}

trait AppendOutputModeTests extends SchemaEnforcementSuiteBase with SharedSparkSession
  with SQLTestUtils {
  import testImplicits._

  testQuietly("reject schema changes by default - streaming") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)

      val memStream = MemoryStream[Long]
      val stream = memStream.toDS().toDF("value1234") // different column name
        .writeStream
        .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
        .format("delta")
        .start(dir.getAbsolutePath)
      try {
        disableAutoMigration {
          val e = intercept[StreamingQueryException] {
            memStream.addData(1L)
            stream.processAllAvailable()
          }
          assert(e.cause.isInstanceOf[AnalysisException])
          assert(e.cause.getMessage.contains(DeltaOptions.MERGE_SCHEMA_OPTION))
        }
      } finally {
        stream.stop()
      }
    }
  }

  testQuietly("reject schema changes when autoMigrate enabled but writer config disabled") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)

      val memStream = MemoryStream[Long]
      val stream = memStream.toDS().toDF("value1234") // different column name
        .writeStream
        .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
        .format("delta")
        .option(DeltaOptions.MERGE_SCHEMA_OPTION, "false")
        .start(dir.getAbsolutePath)
      try {
        enableAutoMigration {
          val e = intercept[StreamingQueryException] {
            memStream.addData(1L)
            stream.processAllAvailable()
          }
          assert(e.cause.isInstanceOf[AnalysisException])
          assert(e.cause.getMessage.contains(DeltaOptions.MERGE_SCHEMA_OPTION))
        }
      } finally {
        stream.stop()
      }
    }
  }

  test("allow schema changes when autoMigrate is enabled - streaming") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)

      enableAutoMigration {
        val memStream = MemoryStream[Long]
        val stream = memStream.toDS().toDF("value1234") // different column name
          .writeStream
          .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
          .format("delta")
          .start(dir.getAbsolutePath)
        try {
          memStream.addData(1L)
          stream.processAllAvailable()

          assert(spark.read.format("delta").load(dir.getAbsolutePath).schema.length == 2)
        } finally {
          stream.stop()
        }
      }
    }
  }

  test("allow schema change with option - streaming") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)

      val memStream = MemoryStream[Long]
      val stream = memStream.toDS().toDF("value1234") // different column name
        .writeStream
        .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
        .option(DeltaOptions.MERGE_SCHEMA_OPTION, "true")
        .format("delta")
        .start(dir.getAbsolutePath)
      try {
        disableAutoMigration {
          memStream.addData(1L)
          stream.processAllAvailable()

          assert(spark.read.format("delta").load(dir.getAbsolutePath).schema.length == 2)
        }
      } finally {
        stream.stop()
      }
    }
  }

  testQuietly("JSON ETL workflow, reject NullTypes") {
    enableAutoMigration {
      val row1 = """{"key":"abc","id":null}"""
      withTempDir { dir =>
        val schema = new StructType().add("key", StringType).add("id", NullType)

        val memStream = MemoryStream[String]
        val stream = memStream.toDS().select(from_json('value, schema).as("value"))
          .select($"value.*")
          .writeStream
          .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
          .format("delta")
          .start(dir.getAbsolutePath)

        try {
          val e = intercept[StreamingQueryException] {
            memStream.addData(row1)
            stream.processAllAvailable()
          }
          assert(e.cause.isInstanceOf[AnalysisException])
          assert(e.cause.getMessage.contains("NullType"))
        } finally {
          stream.stop()
        }
      }
    }
  }

  testQuietly("JSON ETL workflow, reject NullTypes on nested column") {
    enableAutoMigration {
      val row1 = """{"key":"abc","id":{"a":null}}"""
      withTempDir { dir =>
        val schema = new StructType().add("key", StringType)
          .add("id", new StructType().add("a", NullType))

        val memStream = MemoryStream[String]
        val stream = memStream.toDS().select(from_json('value, schema).as("value"))
          .select($"value.*")
          .writeStream
          .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
          .format("delta")
          .start(dir.getAbsolutePath)

        try {
          val e = intercept[StreamingQueryException] {
            memStream.addData(row1)
            stream.processAllAvailable()
          }
          assert(e.cause.isInstanceOf[AnalysisException])
          assert(e.cause.getMessage.contains("NullType"))
        } finally {
          stream.stop()
        }
      }
    }
  }
}

trait OverwriteSaveModeTests extends BatchWriterTest {
  import testImplicits._

  equivalenceTest("reject schema overwrites by default") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(10).write.overwrite(dir)
        val e = intercept[AnalysisException] {
          spark.range(10).withColumn("part", 'id + 1).write.overwrite(dir)
        }
        assert(e.getMessage.contains(DeltaOptions.OVERWRITE_SCHEMA_OPTION))
      }
    }
  }

  equivalenceTest("can overwrite schema when using overwrite mode - option") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").write.overwrite(dir)
        spark.range(5).toDF("value").write.option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
          .overwrite(dir)

        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        assert(df.schema.fieldNames === Array("value"))
      }
    }
  }

  equivalenceTest("when autoMerge sqlConf is enabled, we merge schemas") {
    enableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").write.overwrite(dir)
        spark.range(5).toDF("value").write.overwrite(dir)

        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        assert(df.schema.fieldNames === Array("id", "value"))
      }
    }
  }

  equivalenceTest("reject migration when autoMerge sqlConf is enabled and writer config disabled") {
    enableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").write.overwrite(dir)
        intercept[AnalysisException] {
          spark.range(5).toDF("value").write.option(DeltaOptions.MERGE_SCHEMA_OPTION, "false")
            .overwrite(dir)
        }

        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        assert(df.schema.fieldNames === Array("id"))
      }
    }
  }

  equivalenceTest("schema merging with replaceWhere - sqlConf") {
    enableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").withColumn("part", 'id % 2).write
          .partitionBy("part")
          .overwrite(dir)
        Seq((1L, 0L), (2L, 0L)).toDF("value", "part").write
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "part = 0")
          .overwrite(dir)

        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        assert(df.schema.fieldNames === Array("id", "part", "value"))
      }
    }
  }

  equivalenceTest("schema merging with replaceWhere - option") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").withColumn("part", 'id % 2).write
          .partitionBy("part")
          .overwrite(dir)
        Seq((1L, 0L), (2L, 0L)).toDF("value", "part").write
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "part = 0")
          .option(DeltaOptions.MERGE_SCHEMA_OPTION, "true")
          .overwrite(dir)

        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        assert(df.schema.fieldNames === Array("id", "part", "value"))
      }
    }
  }

  equivalenceTest("schema merging with replaceWhere - option case insensitive") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").withColumn("part", 'id % 2).write
          .partitionBy("part")
          .overwrite(dir)
        Seq((1L, 0L), (2L, 0L)).toDF("value", "part").write
          .option("RePlAcEwHeRe", "part = 0")
          .option("mErGeScHeMa", "true")
          .overwrite(dir)

        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        assert(df.schema.fieldNames === Array("id", "part", "value"))
      }
    }
  }

  equivalenceTest("reject schema merging with replaceWhere - overwrite option") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").withColumn("part", 'id % 2).write
          .partitionBy("part")
          .overwrite(dir)
        val e = intercept[AnalysisException] {
          Seq((1L, 0L), (2L, 0L)).toDF("value", "part").write
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "part = 0")
            .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
            .overwrite(dir)
        }
        assert(e.getMessage.contains(DeltaOptions.MERGE_SCHEMA_OPTION))
      }
    }
  }

  equivalenceTest("reject schema merging with replaceWhere - no option") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").withColumn("part", 'id % 2).write
          .partitionBy("part")
          .overwrite(dir)
        val e = intercept[AnalysisException] {
          Seq((1L, 0L), (2L, 0L)).toDF("value", "part").write
            .partitionBy("part")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "part = 0")
            .overwrite(dir)
        }
        assert(e.getMessage.contains(DeltaOptions.MERGE_SCHEMA_OPTION))
      }
    }
  }

  equivalenceTest("reject schema merging with replaceWhere - option set to false, config true") {
    enableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").withColumn("part", 'id % 2).write
          .partitionBy("part")
          .overwrite(dir)
        val e = intercept[AnalysisException] {
          Seq((1L, 0L), (2L, 0L)).toDF("value", "part").write
            .partitionBy("part")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "part = 0")
            .option(DeltaOptions.MERGE_SCHEMA_OPTION, "false")
            .overwrite(dir)
        }
        assert(e.getMessage.contains(DeltaOptions.MERGE_SCHEMA_OPTION))
      }
    }
  }

  equivalenceTest("reject change partitioning with overwrite - sqlConf") {
    enableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").write
          .overwrite(dir)
        val e = intercept[AnalysisException] {
          spark.range(5).toDF("id").withColumn("part", 'id % 2).write
            .partitionBy("part")
            .overwrite(dir)
        }
        assert(e.getMessage.contains(DeltaOptions.OVERWRITE_SCHEMA_OPTION))

        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.snapshot.metadata.partitionColumns === Nil)
        assert(deltaLog.snapshot.metadata.schema.fieldNames === Array("id"))
      }
    }
  }

  equivalenceTest("can change partitioning with overwrite - option") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").write
          .overwrite(dir)
        spark.range(5).toDF("id").withColumn("part", 'id % 2).write
          .partitionBy("part")
          .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
          .overwrite(dir)

        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.snapshot.metadata.partitionColumns === Seq("part"))
        assert(deltaLog.snapshot.metadata.schema.fieldNames === Array("id", "part"))
      }
    }
  }

  equivalenceTest("can't change partitioning with overwrite and replaceWhere - option") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").withColumn("part", 'id % 2).write
          .partitionBy("part")
          .overwrite(dir)

        intercept[AnalysisException] {
          spark.range(5).toDF("id").withColumn("part", lit(0L)).withColumn("test", 'id + 1).write
            .partitionBy("part", "test")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "part = 0")
            .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
            .overwrite(dir)
        }
      }
    }
  }

  equivalenceTest("can drop columns with overwriteSchema") {
    disableAutoMigration {
      withTempDir { dir =>
        spark.range(5).toDF("id").withColumn("part", 'id % 2).write
          .overwrite(dir)
        spark.range(5).toDF("id").write
          .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
          .overwrite(dir)

        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.snapshot.metadata.partitionColumns === Nil)
        assert(deltaLog.snapshot.metadata.schema.fieldNames === Array("id"))
      }
    }
  }

  equivalenceTest("can change column data type with overwriteSchema") {
    disableAutoMigration {
      withTempDir { dir =>
        val deltaLog = DeltaLog.forTable(spark, dir)
        spark.range(5).toDF("id").write
          .overwrite(dir)
        assert(deltaLog.snapshot.metadata.schema.head === StructField("id", LongType))
        spark.range(5).toDF("id").selectExpr("cast(id as string) as id").write
          .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
          .overwrite(dir)
        assert(deltaLog.snapshot.metadata.schema.head === StructField("id", StringType))
      }
    }
  }

  equivalenceTest("reject columns that only differ by case - overwrite") {
    withTempDir { dir =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        intercept[AnalysisException] {
          spark.range(10).withColumn("ID", 'id + 1).write.overwrite(dir)
        }

        intercept[AnalysisException] {
          spark.range(10).withColumn("ID", 'id + 1).write
            .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
            .overwrite(dir)
        }

        intercept[AnalysisException] {
          spark.range(10).withColumn("a", 'id + 1).write
            .partitionBy("a", "A")
            .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
            .overwrite(dir)
        }
      }
    }
  }
}

trait CompleteOutputModeTests extends SchemaEnforcementSuiteBase with SharedSparkSession
  with SQLTestUtils {
  import testImplicits._

  testQuietly("reject complete mode with new schema by default") {
    disableAutoMigration {
      withTempDir { dir =>
        val memStream = MemoryStream[Long]
        val query = memStream.toDS().toDF("id")
          .withColumn("part", 'id % 3)
          .groupBy("part")
          .count()

        val stream1 = query.writeStream
          .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
          .outputMode("complete")
          .format("delta")
          .start(dir.getAbsolutePath)
        try {
          memStream.addData(1L)
          stream1.processAllAvailable()
        } finally {
          stream1.stop()
        }

        assert(spark.read.format("delta").load(dir.getAbsolutePath).schema.length == 2)

        val stream2 = query.withColumn("test", lit("abc")).writeStream
          .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
          .outputMode("complete")
          .format("delta")
          .start(dir.getAbsolutePath)
        try {
          val e = intercept[StreamingQueryException] {
            memStream.addData(2L)
            stream2.processAllAvailable()
          }
          assert(e.cause.isInstanceOf[AnalysisException])
          assert(e.cause.getMessage.contains(DeltaOptions.OVERWRITE_SCHEMA_OPTION))

        } finally {
          stream2.stop()
        }
      }
    }
  }

  test("complete mode can overwrite schema with option") {
    disableAutoMigration {
      withTempDir { dir =>
        val memStream = MemoryStream[Long]
        val query = memStream.toDS().toDF("id")
          .withColumn("part", 'id % 3)
          .groupBy("part")
          .count()

        val stream1 = query.writeStream
          .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
          .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
          .outputMode("complete")
          .format("delta")
          .start(dir.getAbsolutePath)
        try {
          memStream.addData(1L)
          stream1.processAllAvailable()
        } finally {
          stream1.stop()
        }

        assert(spark.read.format("delta").load(dir.getAbsolutePath).schema.length == 2)

        val stream2 = query.withColumn("test", lit("abc")).writeStream
          .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
          .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
          .outputMode("complete")
          .format("delta")
          .start(dir.getAbsolutePath)
        try {
          memStream.addData(2L)
          stream2.processAllAvailable()

          memStream.addData(3L)
          stream2.processAllAvailable()
        } finally {
          stream2.stop()
        }

        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        assert(df.schema.length == 3)

        val deltaLog = DeltaLog.forTable(spark, dir)
        val lastCommitFile = deltaLog.store.listFrom(FileNames.deltaFile(deltaLog.logPath, 0L))
          .map(_.getPath).filter(FileNames.isDeltaFile).toArray.last
        val lastCommitContainsMetadata = deltaLog.store.read(lastCommitFile)
          .exists(JsonUtils.mapper.readValue[SingleAction](_).metaData != null)

        assert(!lastCommitContainsMetadata,
          "Metadata shouldn't be updated as long as schema doesn't change")

        checkAnswer(
          df,
          Row(0L, 1L, "abc") :: Row(1L, 1L, "abc") :: Row(2L, 1L, "abc") :: Nil)
      }
    }
  }

  test("complete mode behavior with autoMigrate enabled is to migrate schema") {
    enableAutoMigration {
      withTempDir { dir =>
        val memStream = MemoryStream[Long]
        val query = memStream.toDS().toDF("id")
          .withColumn("part", 'id % 3)
          .groupBy("part")
          .count()

        val stream1 = query.writeStream
          .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
          .outputMode("complete")
          .format("delta")
          .start(dir.getAbsolutePath)
        try {
          memStream.addData(1L)
          stream1.processAllAvailable()
        } finally {
          stream1.stop()
        }

        assert(spark.read.format("delta").load(dir.getAbsolutePath).schema.length == 2)

        val stream2 = query.withColumn("test", lit("abc")).writeStream
          .option("checkpointLocation", new File(dir, "_checkpoint").getAbsolutePath)
          .outputMode("complete")
          .format("delta")
          .start(dir.getAbsolutePath)
        try {
          memStream.addData(2L)
          stream2.processAllAvailable()

          memStream.addData(3L)
          stream2.processAllAvailable()
        } finally {
          stream2.stop()
        }

        val df = spark.read.format("delta").load(dir.getAbsolutePath)
        assert(df.schema.length == 3)

        val deltaLog = DeltaLog.forTable(spark, dir)
        val lastCommitFile = deltaLog.store.listFrom(FileNames.deltaFile(deltaLog.logPath, 0L))
          .map(_.getPath).filter(FileNames.isDeltaFile).toArray.last
        val lastCommitContainsMetadata = deltaLog.store.read(lastCommitFile)
          .exists(JsonUtils.mapper.readValue[SingleAction](_).metaData != null)

        assert(!lastCommitContainsMetadata,
          "Metadata shouldn't be updated as long as schema doesn't change")

        checkAnswer(
          df,
          Row(0L, 1L, "abc") :: Row(1L, 1L, "abc") :: Row(2L, 1L, "abc") :: Nil)
      }
    }
  }
}

class SchemaEnforcementWithPathSuite
  extends AppendSaveModeTests
  with AppendSaveModeNullTests
  with OverwriteSaveModeTests {
  override val saveOperation = SaveWithPath()
}

class SchemaEnforcementWithTableSuite
  extends AppendSaveModeTests
  with OverwriteSaveModeTests
  with DeltaSQLCommandTest {

  override val saveOperation = SaveAsTable("delta_schema_test")
}

class SchemaEnforcementStreamingSuite
  extends AppendOutputModeTests
  with CompleteOutputModeTests {
}

