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

import scala.collection.JavaConverters._

import com.databricks.service.IgnoreIfSparkService
import com.databricks.spark.util.CloudUtils
import org.apache.spark.sql.delta.schema.InvariantViolationException

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, CatalogUtils, ExternalCatalogUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StringType, StructType}

class DeltaDDLSuite extends DeltaDDLTestBase
  with DataSourceV2Test {

  override protected def verifyDescribeTable(tblName: String): Unit = {
    val res = sql(s"DESCRIBE TABLE $tblName").collect()
    assert(res.takeRight(2).map(_.getString(1)) === Seq("name", "dept"))
  }

  override protected def verifyNullabilityFailure(exception: AnalysisException): Unit = {
    exception.getMessage.contains("Cannot change nullable column to non-nullable")
  }
}


abstract class DeltaDDLTestBase extends QueryTest with SQLTestUtils {
  import testImplicits._
  import com.databricks.service.SparkServiceTestUtils._

  protected def withCloudProvider(cloudProvider: String)(f: => Unit): Unit = {
    val sparkConf = SparkEnv.get.conf
    val originalCloudProvider = sparkConf.getOption(CloudUtils.CLOUD_PROVIDER_CONF)
    sparkConf.set(CloudUtils.CLOUD_PROVIDER_CONF, cloudProvider)
    try {
      f
    } finally {
      if (originalCloudProvider.isDefined) {
        sparkConf.set(CloudUtils.CLOUD_PROVIDER_CONF, originalCloudProvider.get)
      } else {
        sparkConf.remove(CloudUtils.CLOUD_PROVIDER_CONF)
      }
    }
  }

  protected def verifyDescribeTable(tblName: String): Unit

  protected def verifyNullabilityFailure(exception: AnalysisException): Unit

  testQuietly("create table with NOT NULL - check violation through file writing") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        sql(s"""
               |CREATE TABLE tahoe_test(a LONG, b String NOT NULL)
               |USING tahoe
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = false)
        assert(spark.table("tahoe_test").schema === expectedSchema)

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tahoe_test"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        Seq((1L, "a")).toDF("a", "b")
          .write.format("delta").mode("append").save(table.location.toString)
        val read = spark.read.format("delta").load(table.location.toString)
        checkAnswer(read, Seq(Row(1L, "a")))

        intercept[SparkException] {
          Seq((2L, null)).toDF("a", "b")
            .write.format("delta").mode("append").save(table.location.toString)
        }
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS with NOT NULL - not supported") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        sql(s"""
               |CREATE TABLE tahoe_test(a LONG)
               |USING tahoe
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType().add("a", LongType, nullable = true)
        assert(spark.table("tahoe_test").schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE tahoe_test
               |ADD COLUMNS (b String NOT NULL, c Int)""".stripMargin)
        }
        val msg = "`NOT NULL in ALTER TABLE ADD COLUMNS` is not supported for Delta tables"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from nullable to NOT NULL - not supported") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        sql(s"""
               |CREATE TABLE tahoe_test(a LONG, b String)
               |USING tahoe
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = true)
        assert(spark.table("tahoe_test").schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE tahoe_test
               |CHANGE COLUMN b b String NOT NULL""".stripMargin)
        }
        verifyNullabilityFailure(e)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from NOT NULL to nullable") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        sql(
          s"""
             |CREATE TABLE tahoe_test(a LONG NOT NULL, b String)
             |USING tahoe
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType()
          .add("a", LongType, nullable = false)
          .add("b", StringType, nullable = true)
        assert(spark.table("tahoe_test").schema === expectedSchema)

        sql("INSERT INTO tahoe_test SELECT 1, 'a'")
        checkAnswer(
          sql("SELECT * FROM tahoe_test"),
          Seq(Row(1L, "a")))

        sql(
          s"""
             |ALTER TABLE tahoe_test
             |ALTER COLUMN a DROP NOT NULL""".stripMargin)
        val expectedSchema2 = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = true)
        assert(spark.table("tahoe_test").schema === expectedSchema2)

        sql("INSERT INTO tahoe_test SELECT NULL, 'b'")
        checkAnswer(
          sql("SELECT * FROM tahoe_test"),
          Seq(Row(1L, "a"), Row(null, "b")))
      }
    }
  }

  testQuietly("create table with NOT NULL - check violation through SQL") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        sql(s"""
               |CREATE TABLE tahoe_test(a LONG, b String NOT NULL)
               |USING tahoe
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = false)
        assert(spark.table("tahoe_test").schema === expectedSchema)

        sql("INSERT INTO tahoe_test SELECT 1, 'a'")
        checkAnswer(
          sql("SELECT * FROM tahoe_test"),
          Seq(Row(1L, "a")))

        val e = intercept[Exception] {
          sql("INSERT INTO tahoe_test VALUES (2, null)")
        }
        if (!e.getMessage.contains("nullable values to non-null column")) {
          verifyInvariantViolationException(e)
        }
      }
    }
  }

  testQuietly("create table with NOT NULL in struct type - check violation") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        sql(s"""
               |CREATE TABLE tahoe_test
               |(x struct<a: LONG, b: String NOT NULL>, y LONG)
               |USING tahoe
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("x", new StructType().
            add("a", LongType, nullable = true)
            .add("b", StringType, nullable = false))
          .add("y", LongType, nullable = true)
        assert(spark.table("tahoe_test").schema === expectedSchema)

        sql("INSERT INTO tahoe_test SELECT (1, 'a'), 1")
        checkAnswer(
          sql("SELECT * FROM tahoe_test"),
          Seq(Row(Row(1L, "a"), 1)))

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tahoe_test"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        val schema = new StructType()
          .add("x",
            new StructType()
              .add("a", "bigint")
              .add("b", "string"))
          .add("y", "bigint")
        val e = intercept[SparkException] {
          spark.createDataFrame(
            Seq(Row(Row(2L, null), 2L)).asJava,
            schema
          ).write.format("delta").mode("append").save(table.location.toString)
        }
        verifyInvariantViolationException(e)
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS with NOT NULL in struct type - not supported") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        sql(s"""
               |CREATE TABLE tahoe_test
               |(y LONG)
               |USING tahoe
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("y", LongType, nullable = true)
        assert(spark.table("tahoe_test").schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE tahoe_test
               |ADD COLUMNS (x struct<a: LONG, b: String NOT NULL>, z INT)""".stripMargin)
        }
        val msg = "Operation not allowed: " +
          "`NOT NULL in ALTER TABLE ADD COLUMNS` is not supported for Delta tables"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS to table with existing NOT NULL fields") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        sql(
          s"""
             |CREATE TABLE tahoe_test
             |(y LONG NOT NULL)
             |USING tahoe
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("y", LongType, nullable = false)
        assert(spark.table("tahoe_test").schema === expectedSchema)

        sql(
          s"""
             |ALTER TABLE tahoe_test
             |ADD COLUMNS (x struct<a: LONG, b: String>, z INT)""".stripMargin)
        val expectedSchema2 = new StructType()
          .add("y", LongType, nullable = false)
          .add("x", new StructType()
            .add("a", LongType)
            .add("b", StringType))
          .add("z", IntegerType)
        assert(spark.table("tahoe_test").schema === expectedSchema2)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - not supported") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        sql(s"""
               |CREATE TABLE tahoe_test
               |(x struct<a: LONG, b: String>, y LONG)
               |USING tahoe
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("x", new StructType()
            .add("a", LongType)
            .add("b", StringType))
          .add("y", LongType, nullable = true)
        assert(spark.table("tahoe_test").schema === expectedSchema)

        val e1 = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE tahoe_test
               |CHANGE COLUMN x x struct<a: LONG, b: String NOT NULL>""".stripMargin)
        }
        assert(e1.getMessage.contains("Cannot update"))
        val e2 = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE tahoe_test
               |CHANGE COLUMN x.b b String NOT NULL""".stripMargin) // this syntax may change
        }
        verifyNullabilityFailure(e2)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - relaxed") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir { dir =>
        withTable("tahoe_test") {
          sql(
            s"""
               |CREATE TABLE tahoe_test
               |(x struct<a: LONG, b: String NOT NULL> NOT NULL, y LONG)
               |USING tahoe
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
          val expectedSchema = new StructType()
            .add("x", new StructType()
              .add("a", LongType)
              .add("b", StringType, nullable = false), nullable = false)
            .add("y", LongType)
          assert(spark.table("tahoe_test").schema === expectedSchema)
          sql("INSERT INTO tahoe_test SELECT (1, 'a'), 1")
          checkAnswer(
            sql("SELECT * FROM tahoe_test"),
            Seq(Row(Row(1L, "a"), 1)))

          val e1 = intercept[AnalysisException] {
            sql(
              s"""
                 |ALTER TABLE tahoe_test
                 |CHANGE COLUMN x x struct<a: LONG, b: String> NOT NULL""".stripMargin)
          }
          assert(e1.getMessage.contains("Cannot update"))
          sql(
            s"""
               |ALTER TABLE tahoe_test
               |ALTER COLUMN x.b DROP NOT NULL""".stripMargin) // relax nullability
          sql("INSERT INTO tahoe_test SELECT (2, null), null")
          checkAnswer(
            sql("SELECT * FROM tahoe_test"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null)))

          sql(
            s"""
               |ALTER TABLE tahoe_test
               |ALTER COLUMN x DROP NOT NULL""".stripMargin)
          sql("INSERT INTO tahoe_test SELECT null, 3")
          checkAnswer(
            sql("SELECT * FROM tahoe_test"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null),
              Row(null, 3)))
        }
      }
    }
  }

  private def verifyInvariantViolationException(e: Exception): Unit = {
    var violationException = e.getCause
    while (violationException != null &&
      !violationException.isInstanceOf[InvariantViolationException]) {
      violationException = violationException.getCause
    }
    if (violationException == null) {
      fail("Didn't receive a InvariantViolationException.")
    }
    assert(violationException.getMessage.contains("Invariant NOT NULL violated for column"))
  }

  test("ALTER TABLE RENAME TO") {
    withTable("tbl", "newTbl") {
      sql(s"""
            |CREATE TABLE tbl
            |USING tahoe
            |AS SELECT 1 as a, 'a' as b
           """.stripMargin)

      withCloudProvider("AWS") {
        withSQLConf(DatabricksSQLConf.DELTA_ALTER_TABLE_RENAME_ENABLED_ON_AWS.key -> "false") {
          val e = intercept[AnalysisException] {
            sql(s"ALTER TABLE tbl RENAME TO newTbl")
          }
          assert(e.getMessage.contains("ALTER TABLE RENAME TO is not allowed"))
        }
      }

      sql(s"ALTER TABLE tbl RENAME TO newTbl")
      checkDatasetUnorderly(
        sql("SELECT * FROM newTbl").as[(Long, String)],
        1L -> "a")
    }
  }

  test("SHOW CREATE TABLE should not include OPTIONS except for path",
      // TODO(youngbink): remove tag
      IgnoreIfSparkService("accessing physical plan")) {
    withTable("tahoe_test") {
      sql(s"""
             |CREATE TABLE tahoe_test(a LONG, b String)
             |USING tahoe
           """.stripMargin)

      val statement = sql("SHOW CREATE TABLE tahoe_test").collect()(0).getString(0)
      assert(!statement.contains("OPTION"))
    }

    withTempDir { dir =>
      withTable("tahoe_test") {
        val path = dir.getCanonicalPath()
        sql(s"""
               |CREATE TABLE tahoe_test(a LONG, b String)
               |USING tahoe
               |LOCATION '$path'
             """.stripMargin)

        val statement = sql("SHOW CREATE TABLE tahoe_test").collect()(0).getString(0)
        assert(statement.contains(
          s"LOCATION '${CatalogUtils.URIToString(makeQualifiedPath(path))}'"))
        assert(!statement.contains("OPTION"))
      }
    }
  }

  test("DESCRIBE TABLE for partitioned table") {
    withTempDir { dir =>
      withTable("tahoe_test") {
        val path = dir.getCanonicalPath()

        val df = Seq(
          (1, "IT", "Alice"),
          (2, "CS", "Bob"),
          (3, "IT", "Carol")).toDF("id", "dept", "name")
        df.write.format("delta").partitionBy("name", "dept").save(path)

        sql(s"CREATE TABLE tahoe_test USING tahoe LOCATION '$path'")

        verifyDescribeTable("tahoe_test")
        verifyDescribeTable(s"tahoe.`$path`")
      }
    }
  }

  test("drop managed Delta table should invalid DeltaLog cache") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test USING delta AS SELECT 'foo' as a")
      val tableLocation = sql("DESC DETAIL delta_test").select("location").as[String].head()
      val deltaLog = DeltaLog.forTable(spark, tableLocation)
      sql("DROP TABLE delta_test")
      assert(deltaLog ne disableSparkService(DeltaLog.forTable(spark, tableLocation)))
    }
  }

  test("rename managed Delta table should invalid DeltaLog cache") {
    withTable("delta_test", "delta_test2") {
      sql("CREATE TABLE delta_test USING delta AS SELECT 'foo' as a")
      val tableLocation = sql("DESC DETAIL delta_test").select("location").as[String].head()
      val deltaLog = DeltaLog.forTable(spark, tableLocation)
      sql("ALTER TABLE delta_test RENAME TO delta_test2")
      assert(deltaLog ne disableSparkService(DeltaLog.forTable(spark, tableLocation)))
    }
  }
}
