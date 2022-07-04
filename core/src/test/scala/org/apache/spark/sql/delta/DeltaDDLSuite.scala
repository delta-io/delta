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

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class DeltaDDLSuite extends DeltaDDLTestBase with SharedSparkSession  with DeltaSQLCommandTest {

  override protected def verifyDescribeTable(tblName: String): Unit = {
    val res = sql(s"DESCRIBE TABLE $tblName").collect()
    assert(res.takeRight(2).map(_.getString(1)) === Seq("name", "dept"))
  }

  override protected def verifyNullabilityFailure(exception: AnalysisException): Unit = {
    exception.getMessage.contains("Cannot change nullable column to non-nullable")
  }

  test("table creation with ambiguous paths only allowed with legacy flag") {
    // ambiguous paths not allowed
    withTempDir { foo =>
      withTempDir { bar =>
          val fooPath = foo.getCanonicalPath()
          val barPath = bar.getCanonicalPath()
          val e = intercept[AnalysisException] {
            sql(s"CREATE TABLE delta.`$fooPath`(id LONG) USING delta LOCATION '$barPath'")
          }
          assert(e.message.contains("legacy.allowAmbiguousPathsInCreateTable"))
      }
    }

    // allowed with legacy flag
    withTempDir { foo =>
      withTempDir { bar =>
        val fooPath = foo.getCanonicalPath()
        val barPath = bar.getCanonicalPath()
        withSQLConf(DeltaSQLConf.DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS.key -> "true") {
          sql(s"CREATE TABLE delta.`$fooPath`(id LONG) USING delta LOCATION '$barPath'")
          assert(io.delta.tables.DeltaTable.isDeltaTable(fooPath))
          assert(!io.delta.tables.DeltaTable.isDeltaTable(barPath))
        }
      }
    }

    // allowed if paths are the same
    withTempDir { foo =>
      val fooPath = foo.getCanonicalPath()
      sql(s"CREATE TABLE delta.`$fooPath`(id LONG) USING delta LOCATION '$fooPath'")
      assert(io.delta.tables.DeltaTable.isDeltaTable(fooPath))
    }
  }
}


class DeltaDDLNameColumnMappingSuite extends DeltaDDLSuite
  with DeltaColumnMappingEnableNameMode {

  override protected def runOnlyTests = Seq(
    "create table with NOT NULL - check violation through file writing",
    "ALTER TABLE CHANGE COLUMN with nullability change in struct type - relaxed"
  )
}


abstract class DeltaDDLTestBase extends QueryTest with SQLTestUtils {
  import testImplicits._

  protected def verifyDescribeTable(tblName: String): Unit

  protected def verifyNullabilityFailure(exception: AnalysisException): Unit

  protected def getDeltaLog(tableLocation: String): DeltaLog = {
      DeltaLog.forTable(spark, tableLocation)
  }


  testQuietly("create table with NOT NULL - check violation through file writing") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String NOT NULL)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = false)
        assert(spark.table("delta_test").schema === expectedSchema)

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("delta_test"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        Seq((1L, "a")).toDF("a", "b")
          .write.format("delta").mode("append").save(table.location.toString)
        val read = spark.read.format("delta").load(table.location.toString)
        checkAnswer(read, Seq(Row(1L, "a")))

        intercept[InvariantViolationException] {
          Seq((2L, null)).toDF("a", "b")
            .write.format("delta").mode("append").save(table.location.toString)
        }
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS with NOT NULL - not supported") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType().add("a", LongType, nullable = true)
        assert(spark.table("delta_test").schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE delta_test
               |ADD COLUMNS (b String NOT NULL, c Int)""".stripMargin)
        }
        val msg = "`NOT NULL in ALTER TABLE ADD COLUMNS` is not supported for Delta tables"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from nullable to NOT NULL - not supported") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = true)
        assert(spark.table("delta_test").schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE delta_test
               |CHANGE COLUMN b b String NOT NULL""".stripMargin)
        }
        verifyNullabilityFailure(e)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from NOT NULL to nullable") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(
          s"""
             |CREATE TABLE delta_test(a LONG NOT NULL, b String)
             |USING delta
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType()
          .add("a", LongType, nullable = false)
          .add("b", StringType, nullable = true)
        assert(spark.table("delta_test").schema === expectedSchema)

        sql("INSERT INTO delta_test SELECT 1, 'a'")
        checkAnswer(
          sql("SELECT * FROM delta_test"),
          Seq(Row(1L, "a")))

        sql(
          s"""
             |ALTER TABLE delta_test
             |ALTER COLUMN a DROP NOT NULL""".stripMargin)
        val expectedSchema2 = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = true)
        assert(spark.table("delta_test").schema === expectedSchema2)

        sql("INSERT INTO delta_test SELECT NULL, 'b'")
        checkAnswer(
          sql("SELECT * FROM delta_test"),
          Seq(Row(1L, "a"), Row(null, "b")))
      }
    }
  }

  testQuietly("create table with NOT NULL - check violation through SQL") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String NOT NULL)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = false)
        assert(spark.table("delta_test").schema === expectedSchema)

        sql("INSERT INTO delta_test SELECT 1, 'a'")
        checkAnswer(
          sql("SELECT * FROM delta_test"),
          Seq(Row(1L, "a")))

        val e = intercept[InvariantViolationException] {
          sql("INSERT INTO delta_test VALUES (2, null)")
        }
        if (!e.getMessage.contains("nullable values to non-null column")) {
          verifyInvariantViolationException(e)
        }
      }
    }
  }

  testQuietly("create table with NOT NULL in struct type - check violation") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test
               |(x struct<a: LONG, b: String NOT NULL>, y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("x", new StructType().
            add("a", LongType, nullable = true)
            .add("b", StringType, nullable = false))
          .add("y", LongType, nullable = true)
        assert(spark.table("delta_test").schema === expectedSchema)

        sql("INSERT INTO delta_test SELECT (1, 'a'), 1")
        checkAnswer(
          sql("SELECT * FROM delta_test"),
          Seq(Row(Row(1L, "a"), 1)))

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("delta_test"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        val schema = new StructType()
          .add("x",
            new StructType()
              .add("a", "bigint")
              .add("b", "string"))
          .add("y", "bigint")
        val e = intercept[InvariantViolationException] {
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
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test
               |(y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("y", LongType, nullable = true)
        assert(spark.table("delta_test").schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE delta_test
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
      withTable("delta_test") {
        sql(
          s"""
             |CREATE TABLE delta_test
             |(y LONG NOT NULL)
             |USING delta
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("y", LongType, nullable = false)
        assert(spark.table("delta_test").schema === expectedSchema)

        sql(
          s"""
             |ALTER TABLE delta_test
             |ADD COLUMNS (x struct<a: LONG, b: String>, z INT)""".stripMargin)
        val expectedSchema2 = new StructType()
          .add("y", LongType, nullable = false)
          .add("x", new StructType()
            .add("a", LongType)
            .add("b", StringType))
          .add("z", IntegerType)
        assert(spark.table("delta_test").schema === expectedSchema2)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - not supported") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test
               |(x struct<a: LONG, b: String>, y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("x", new StructType()
            .add("a", LongType)
            .add("b", StringType))
          .add("y", LongType, nullable = true)
        assert(spark.table("delta_test").schema === expectedSchema)

        val e1 = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE delta_test
               |CHANGE COLUMN x x struct<a: LONG, b: String NOT NULL>""".stripMargin)
        }
        assert(e1.getMessage.contains("Cannot update"))
        val e2 = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE delta_test
               |CHANGE COLUMN x.b b String NOT NULL""".stripMargin) // this syntax may change
        }
        verifyNullabilityFailure(e2)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - relaxed") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir { dir =>
        val tblName = "delta_test2"
        withTable(tblName) {
          sql(
            s"""
               |CREATE TABLE $tblName
               |(x struct<a: LONG, b: String NOT NULL> NOT NULL, y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
          val expectedSchema = new StructType()
            .add("x", new StructType()
              .add("a", LongType)
              .add("b", StringType, nullable = false), nullable = false)
            .add("y", LongType)
          assert(spark.table(tblName).schema === expectedSchema)
          sql(s"INSERT INTO $tblName SELECT (1, 'a'), 1")
          checkAnswer(
            sql(s"SELECT * FROM $tblName"),
            Seq(Row(Row(1L, "a"), 1)))

          sql(
            s"""
               |ALTER TABLE $tblName
               |ALTER COLUMN x.b DROP NOT NULL""".stripMargin) // relax nullability
          sql(s"INSERT INTO $tblName SELECT (2, null), null")
          checkAnswer(
            sql(s"SELECT * FROM $tblName"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null)))

          sql(
            s"""
               |ALTER TABLE $tblName
               |ALTER COLUMN x DROP NOT NULL""".stripMargin)
          sql(s"INSERT INTO $tblName SELECT null, 3")
          checkAnswer(
            sql(s"SELECT * FROM $tblName"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null),
              Row(null, 3)))
        }
      }
    }
  }

  private def verifyInvariantViolationException(e: InvariantViolationException): Unit = {
    if (e == null) {
      fail("Didn't receive a InvariantViolationException.")
    }
    assert(e.getMessage.contains("NOT NULL constraint violated for column"))
  }

  test("ALTER TABLE RENAME TO") {
    withTable("tbl", "newTbl") {
      sql(s"""
            |CREATE TABLE tbl
            |USING delta
            |AS SELECT 1 as a, 'a' as b
           """.stripMargin)


      sql(s"ALTER TABLE tbl RENAME TO newTbl")
      checkDatasetUnorderly(
        sql("SELECT * FROM newTbl").as[(Long, String)],
        1L -> "a")
    }
  }

  /**
   * Although Spark 3.2 adds the support for SHOW CREATE TABLE for v2 tables, it doesn't work
   * properly for Delta. For example, table properties, constraints and generated columns are not
   * showed properly.
   *
   * TODO Implement Delta's own ShowCreateTableCommand to show the Delta table definition correctly
   */
  test("SHOW CREATE TABLE is not supported") {
    withTable("delta_test") {
      sql(
        s"""
           |CREATE TABLE delta_test(a LONG, b String)
           |USING delta
           """.stripMargin)

      val e = intercept[AnalysisException] {
        sql("SHOW CREATE TABLE delta_test").collect()(0).getString(0)
      }
      assert(e.message.contains("`SHOW CREATE TABLE` is not supported for Delta table"))
    }

    withTempDir { dir =>
      withTable("delta_test") {
        val path = dir.getCanonicalPath()
        sql(
          s"""
             |CREATE TABLE delta_test(a LONG, b String)
             |USING delta
             |LOCATION '$path'
             """.stripMargin)

        val e = intercept[AnalysisException] {
          sql("SHOW CREATE TABLE delta_test").collect()(0).getString(0)
        }
        assert(e.message.contains("`SHOW CREATE TABLE` is not supported for Delta table"))
      }
    }
  }


  test("DESCRIBE TABLE for partitioned table") {
    withTempDir { dir =>
      withTable("delta_test") {
        val path = dir.getCanonicalPath()

        val df = Seq(
          (1, "IT", "Alice"),
          (2, "CS", "Bob"),
          (3, "IT", "Carol")).toDF("id", "dept", "name")
        df.write.format("delta").partitionBy("name", "dept").save(path)

        sql(s"CREATE TABLE delta_test USING delta LOCATION '$path'")

        verifyDescribeTable("delta_test")
        verifyDescribeTable(s"delta.`$path`")

        assert(sql("DESCRIBE EXTENDED delta_test").collect().length > 0)
      }
    }
  }

  test("snapshot returned after a dropped managed table should be empty") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test USING delta AS SELECT 'foo' as a")
      val tableLocation = sql("DESC DETAIL delta_test").select("location").as[String].head()
      val snapshotBefore = getDeltaLog(tableLocation).update()
      sql("DROP TABLE delta_test")
      val snapshotAfter = getDeltaLog(tableLocation).update()
      assert(snapshotBefore ne snapshotAfter)
      assert(snapshotAfter.version === -1)
    }
  }

  test("snapshot returned after renaming a managed table should be empty") {
    withTable("delta_test", "delta_test2") {
      sql("CREATE TABLE delta_test USING delta AS SELECT 'foo' as a")
      val tableLocation = sql("DESC DETAIL delta_test").select("location").as[String].head()
      val snapshotBefore = getDeltaLog(tableLocation).update()
      sql("ALTER TABLE delta_test RENAME TO delta_test2")
      val snapshotAfter = getDeltaLog(tableLocation).update()
      assert(snapshotBefore ne snapshotAfter)
      assert(snapshotAfter.version === -1)
    }
  }

}
