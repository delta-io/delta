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
import java.io.File

import org.apache.spark.sql.delta.DeltaConfigs.CHECKPOINT_INTERVAL
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

trait DeltaAlterTableTestBase
  extends QueryTest
  with SharedSparkSession
  with DeltaColumnMappingTestUtils
  with DeltaTestUtilsForTempViews {

  protected def createTable(schema: String, tblProperties: Map[String, String]): String

  protected def createTable(df: DataFrame, partitionedBy: Seq[String]): String

  protected def dropTable(identifier: String): Unit

  protected def getDeltaLogWithSnapshot(identifier: String): (DeltaLog, Snapshot)

  final protected def withDeltaTable(schema: String)(f: String => Unit): Unit = {
    withDeltaTable(schema, Map.empty[String, String])(i => f(i))
  }

  final protected def withDeltaTable(
      schema: String,
      tblProperties: Map[String, String])(f: String => Unit): Unit = {
    val identifier = createTable(schema, tblProperties)
    try {
      f(identifier)
    } finally {
      dropTable(identifier)
    }
  }

  final protected def withDeltaTable(df: DataFrame)(f: String => Unit): Unit = {
    withDeltaTable(df, Seq.empty[String])(i => f(i))
  }

  final protected def withDeltaTable(
      df: DataFrame,
      partitionedBy: Seq[String])(f: String => Unit): Unit = {
    val identifier = createTable(df, partitionedBy)
    try {
      f(identifier)
    } finally {
      dropTable(identifier)
    }
  }

  protected def ddlTest(testName: String)(f: => Unit): Unit = {
    testQuietly(testName)(f)
  }

  protected def assertNotSupported(command: String, messages: String*): Unit = {
    val ex = intercept[Exception] {
      sql(command)
    }.getMessage
    assert(ex.contains("not supported") || ex.contains("Unsupported") || ex.contains("Cannot"))
    messages.foreach(msg => assert(ex.contains(msg)))
  }
}

trait DeltaAlterTableTests extends DeltaAlterTableTestBase {

  import testImplicits._

  ///////////////////////////////
  // SET/UNSET TBLPROPERTIES
  ///////////////////////////////

  ddlTest("SET/UNSET TBLPROPERTIES - simple") {
    withDeltaTable("v1 int, v2 string") { tableName =>

      sql(s"""
        |ALTER TABLE $tableName
        |SET TBLPROPERTIES (
        |  'delta.logRetentionDuration' = '2 weeks',
        |  'delta.checkpointInterval' = '20',
        |  'key' = 'value'
        |)""".stripMargin)

      val (deltaLog, snapshot1) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot1.metadata.configuration, Map(
        "delta.logRetentionDuration" -> "2 weeks",
        "delta.checkpointInterval" -> "20",
        "key" -> "value"))
      assert(deltaLog.deltaRetentionMillis(snapshot1.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot1.metadata) == 20)

      sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('delta.checkpointInterval', 'key')")

      val snapshot2 = deltaLog.update()
      assertEqual(snapshot2.metadata.configuration,
        Map("delta.logRetentionDuration" -> "2 weeks"))
      assert(deltaLog.deltaRetentionMillis(snapshot2.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot2.metadata) ==
        CHECKPOINT_INTERVAL.fromString(CHECKPOINT_INTERVAL.defaultValue))
    }
  }

  testQuietlyWithTempView("negative case - not supported on temp views") { isSQLTempView =>
    withDeltaTable("v1 int, v2 string") { tableName =>
      createTempViewFromTable(tableName, isSQLTempView)

      val e = intercept[AnalysisException] {
        sql(
          """
            |ALTER TABLE v
            |SET TBLPROPERTIES (
            |  'delta.logRetentionDuration' = '2 weeks',
            |  'delta.checkpointInterval' = '20',
            |  'key' = 'value'
            |)""".stripMargin)
      }
      assert(e.getMessage.contains("expects a table. Please use ALTER VIEW instead.") ||
        e.getMessage.contains("EXPECT_TABLE_NOT_VIEW.USE_ALTER_VIEW"))
    }
  }

  ddlTest("SET/UNSET TBLPROPERTIES - case insensitivity") {
    withDeltaTable("v1 int, v2 string") { tableName =>

      sql(s"""
        |ALTER TABLE $tableName
        |SET TBLPROPERTIES (
        |  'dEltA.lOgrEteNtiOndURaTion' = '1 weeks',
        |  'DelTa.ChEckPoiNtinTervAl' = '5',
        |  'key' = 'value1'
        |)""".stripMargin)

      val (deltaLog, snapshot1) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot1.metadata.configuration, Map(
        "delta.logRetentionDuration" -> "1 weeks",
        "delta.checkpointInterval" -> "5",
        "key" -> "value1"))
      assert(deltaLog.deltaRetentionMillis(snapshot1.metadata) == 1 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot1.metadata) == 5)

      sql(s"""
        |ALTER TABLE $tableName
        |SET TBLPROPERTIES (
        |  'dEltA.lOgrEteNtiOndURaTion' = '2 weeks',
        |  'DelTa.ChEckPoiNtinTervAl' = '20',
        |  'kEy' = 'value2'
        |)""".stripMargin)

      val snapshot2 = deltaLog.update()
      assertEqual(snapshot2.metadata.configuration, Map(
        "delta.logRetentionDuration" -> "2 weeks",
        "delta.checkpointInterval" -> "20",
        "key" -> "value1",
        "kEy" -> "value2"))
      assert(deltaLog.deltaRetentionMillis(snapshot2.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot2.metadata) == 20)

      sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('DelTa.ChEckPoiNtinTervAl', 'kEy')")

      val snapshot3 = deltaLog.update()
      assertEqual(snapshot3.metadata.configuration,
        Map("delta.logRetentionDuration" -> "2 weeks", "key" -> "value1"))
      assert(deltaLog.deltaRetentionMillis(snapshot3.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot3.metadata) ==
        CHECKPOINT_INTERVAL.fromString(CHECKPOINT_INTERVAL.defaultValue))
    }
  }

  ddlTest("SET/UNSET TBLPROPERTIES - set unknown config") {
    withDeltaTable("v1 int, v2 string") { tableName =>
      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('delta.key' = 'value')")
      }
      assert(ex.getMessage.contains("Unknown configuration was specified: delta.key"))
    }
  }

  ddlTest("SET/UNSET TBLPROPERTIES - set invalid value") {
    withDeltaTable("v1 int, v2 string") { tableName =>

      val ex1 = intercept[Exception] {
        sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('delta.randomPrefixLength' = '-1')")
      }
      assert(ex1.getMessage.contains("randomPrefixLength needs to be greater than 0."))

      val ex2 = intercept[Exception] {
        sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('delta.randomPrefixLength' = 'value')")
      }
      assert(ex2.getMessage.contains("randomPrefixLength needs to be greater than 0."))
    }
  }

  test("SET/UNSET comment by TBLPROPERTIES") {
    withDeltaTable("v1 int, v2 string") { tableName =>
      def assertCommentEmpty(): Unit = {
        val props = sql(s"DESC EXTENDED $tableName").collect()
        assert(!props.exists(_.getString(0) === "Comment"), "Comment should be empty")

        val desc = sql(s"DESCRIBE DETAIL $tableName").head()
        val fieldIndex = desc.fieldIndex("description")
        assert(desc.isNullAt(fieldIndex))
      }

      assertCommentEmpty()

      sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('comment'='does it work?')")

      val props = sql(s"DESC EXTENDED $tableName").collect()
      assert(props.exists(r => r.getString(0) === "Comment" && r.getString(1) === "does it work?"),
        s"Comment not found in:\n${props.mkString("\n")}")

      val desc = sql(s"DESCRIBE DETAIL $tableName").head()
      assert(desc.getAs[String]("description") === "does it work?")

      sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('comment')")
      assertCommentEmpty()
    }
  }

  test("update comment by TBLPROPERTIES") {
    val tableName = "comment_table"

    def checkComment(expected: String): Unit = {
      val props = sql(s"DESC EXTENDED $tableName").collect()
      assert(props.exists(r => r.getString(0) === "Comment" && r.getString(1) === expected),
        s"Comment not found in:\n${props.mkString("\n")}")

      val desc = sql(s"DESCRIBE DETAIL $tableName").head()
      assert(desc.getAs[String]("description") === expected)
    }

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id bigint) USING delta COMMENT 'x'")
      checkComment("x")

      sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('comment'='y')")

      checkComment("y")
    }
  }

  ddlTest("Invalid TBLPROPERTIES") {
    withDeltaTable("v1 int, v2 string") { tableName =>
      // Handled by Spark
      intercept[ParseException] {
        sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('location'='/some/new/path')")
      }
      // Handled by Spark
      intercept[ParseException] {
        sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('provider'='json')")
      }
      // Illegal to add constraints
      val e3 = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('delta.constraints.c1'='age >= 25')")
      }
      assert(e3.getMessage.contains("ALTER TABLE ADD CONSTRAINT"))
    }
  }

  private def setProps(table: String, kvs: (String, String)*): Unit = {
    val props = kvs.map { case (k, v) => s"'$k'='$v'" }.mkString(", ")
    val sqlString = s"ALTER TABLE $table SET TBLPROPERTIES ($props)"
    spark.sql(sqlString)
  }

  private def expectValidationError(f: => Unit): Unit = {
    val ex = intercept[Exception](f)
    assert(
      (ex.getMessage.contains("delta.logRetentionDuration") &&
      ex.getMessage.contains("delta.deletedFileRetentionDuration")) &&
      (ex.getMessage.contains("needs to be greater than or equal to") ||
        ex.getMessage.contains("needs to be less than or equal to"))
    )
  }

  ///////////////////////////////
  // logRetentionDuration and deletedFileRetentionDuration table property
  // compatibility tests
  ///////////////////////////////

  // cases where validation succeeds
  test("log > deleted (same units) succeeds") {
    withDeltaTable("v1 int, v2 string") { t =>
      setProps(t,
        "delta.deletedFileRetentionDuration" -> "interval 7 days",
        "delta.logRetentionDuration"         -> "interval 30 days"
      )
    }
  }

  test("log > deleted (different units) succeeds") {
    withDeltaTable("v1 int, v2 string") { t =>
      setProps(t,
        "delta.deletedFileRetentionDuration" -> "interval 4 days",
        "delta.logRetentionDuration"         -> "interval 120 hours"
      )
    }
  }

  test("log > deleted one after the other succeeds") {
    withDeltaTable("v1 int, v2 string") { t =>
      setProps(t,
        "delta.deletedFileRetentionDuration" -> "interval 6 days"
      )
      setProps(t,
        "delta.logRetentionDuration" -> "interval 10 days"
      )
    }
  }

  test("key case-insensitivity still succeeds") {
    withDeltaTable("v1 int, v2 string") { t =>
      setProps(t,
        "delta.deletedFileRETENTIONDuration" -> "  interval 7 days ",
        "delta.logRetentionDURATION"         -> " INTERVAL 30 DAYS "
      )
    }
  }

  test("equal durations shouldn't fail") {
    withDeltaTable("v1 int, v2 string") { t =>
        setProps(t,
          "delta.deletedFileRetentionDuration" -> "interval 7 days",
          "delta.logRetentionDuration"         -> "interval 1 week"
        )
    }
  }

  // cases where validation fails
  test("log < deleted should fail") {
    withDeltaTable("v1 int, v2 string") { t =>
      expectValidationError(
        setProps(t,
          "delta.deletedFileRetentionDuration" -> "interval 10 days",
          "delta.logRetentionDuration"         -> "interval 6 days"
        )
      )
    }
  }

  test("sequence that becomes invalid (raise deleted above log) should fail") {
    withDeltaTable("v1 int, v2 string") { t =>
      setProps(t,
        "delta.deletedFileRetentionDuration" -> "interval 7 days",
        "delta.logRetentionDuration"         -> "interval 30 days"
      )
      expectValidationError(
        setProps(t, "delta.deletedFileRetentionDuration" -> "interval 60 days")
      )
    }
  }

  test("default log vs explicit deleted that exceeds default should fail") {
    withDeltaTable("v1 int, v2 string") { t =>
      // default log 30 days; setting deleted to 45 should fail
      expectValidationError(
        setProps(t, "delta.deletedFileRetentionDuration" -> "interval 45 days")
      )
    }
  }

  test("default deletedRetention vs explicit log retention that exceeds default should fail") {
    withDeltaTable("v1 int, v2 string") { t =>
      // default deletedFileRetention 7 days; setting log to 5 should fail
      expectValidationError(
        setProps(t, "delta.logRetentionDuration" -> "interval 5 days")
      )
    }
  }

  test("key case-insensitivity still fails") {
    withDeltaTable("v1 int, v2 string") { t =>
      expectValidationError(
        setProps(t,
          "DELTA.DELETEDFILERETENTIONDURATION" -> "interval 14 days",
          "delta.logRetentionDurATION"         -> "interval 7 days"
        )
      )
    }
  }

  test("reset to defaults becomes valid") {
    withDeltaTable("v1 int, v2 string") { t =>
      // Start invalid
      expectValidationError(
        setProps(t,
          "delta.deletedFileRetentionDuration" -> "interval 40 days",
          "delta.logRetentionDuration"         -> "interval 30 days"
        )
      )
      // Reset deleted; expect success if defaults are valid
      spark.sql(s"ALTER TABLE $t UNSET TBLPROPERTIES ('delta.deletedFileRetentionDuration')")
      // Now set log to something valid relative to default deleted (7d)
      setProps(t, "delta.logRetentionDuration" -> "interval 30 days")
    }
  }

  test("property values are invalid before. Setting an unrelated property shouldn't error out") {
    withDeltaTable("v1 int, v2 string") { t =>
      // Start invalid
      withSQLConf(
        DeltaSQLConf.ENFORCE_DELETED_FILE_AND_LOG_RETENTION_DURATION_COMPATIBILITY.key ->
          false.toString) {
        setProps(t,
          "delta.deletedFileRetentionDuration" -> "interval 40 days",
          "delta.logRetentionDuration" -> "interval 30 days"
        )
      }
      // Now set unrelated table property
      setProps(t, "delta.checkpointInterval" -> "100")
    }
  }

  ///////////////////////////////
  // ADD COLUMNS
  ///////////////////////////////

  ddlTest("ADD COLUMNS - simple") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>
      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long, v4 double)")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long").add("v4", "double"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, Option[Long], Option[Double])],
        (1, "a", None, None), (2, "b", None, None))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Array") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("a", array(struct("v1")))) { tableName =>
      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (a.element.v3 long)
         """.stripMargin)

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(new StructType()
          .add("v1", "integer")
          .add("v3", "long"))))

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (a.element.v4 struct<f1:long>)
         """.stripMargin)

      assertEqual(deltaLog.snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(new StructType()
          .add("v1", "integer")
          .add("v3", "long")
          .add("v4", new StructType().add("f1", "long")))))

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (a.element.v4.f2 string)
         """.stripMargin)

      assertEqual(deltaLog.snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(new StructType()
          .add("v1", "integer")
          .add("v3", "long")
          .add("v4", new StructType()
            .add("f1", "long")
            .add("f2", "string")))))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Map with simple key") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map('v1, struct("v2")))) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (m.value.mvv3 long)
         """.stripMargin)

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(IntegerType,
          new StructType()
            .add("v2", "string")
            .add("mvv3", "long"))))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Map with simple value") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), 'v2))) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (m.key.mkv3 long)
         """.stripMargin)

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer")
            .add("mkv3", "long"),
          StringType)))
    }
  }

  private def checkErrMsg(msg: String, field: Seq[String]): Unit = {
    val fieldStr = field.map(f => s"`$f`").mkString(".")
    val fieldParentStr = field.dropRight(1).map(f => s"`$f`").mkString(".")
    assert(msg.contains(
      s"Field name $fieldStr is invalid: $fieldParentStr is not a struct"))
  }

  ddlTest("ADD COLUMNS should not be able to add column to basic type key/value of " +
    "MapType") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map('v1, 'v2))) { tableName =>
      var ex = intercept[AnalysisException] {
        sql(
          s"""
             |ALTER TABLE $tableName ADD COLUMNS (m.key.mkv3 long)
         """.stripMargin)
      }
      checkErrMsg(ex.getMessage, Seq("m", "key", "mkv3"))

      ex = intercept[AnalysisException] {
        sql(
          s"""
             |ALTER TABLE $tableName ADD COLUMNS (m.value.mkv3 long)
         """.stripMargin)
      }
      checkErrMsg(ex.getMessage, Seq("m", "value", "mkv3"))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Map") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), struct("v2")))) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS (m.key.mkv3 long, m.value.mvv3 long)
         """.stripMargin)

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer")
            .add("mkv3", "long"),
          new StructType()
            .add("v2", "string")
            .add("mvv3", "long"))))
    }
  }

  ddlTest("ADD COLUMNS into complex types - Map (nested)") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), struct("v2")))) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS
           |(m.key.mkv3 long, m.value.mvv3 struct<f1: long, f2:array<struct<n:long>>>)
         """.stripMargin)

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer")
            .add("mkv3", "long"),
          new StructType()
            .add("v2", "string")
            .add("mvv3", new StructType()
              .add("f1", "long")
              .add("f2", ArrayType(new StructType()
                .add("n", "long")))))))

      sql(
        s"""
           |ALTER TABLE $tableName ADD COLUMNS
           |(m.value.mvv3.f2.element.p string)
         """.stripMargin)

      assertEqual(deltaLog.snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer")
            .add("mkv3", "long"),
          new StructType()
            .add("v2", "string")
            .add("mvv3", new StructType()
              .add("f1", "long")
              .add("f2", ArrayType(new StructType()
                .add("n", "long")
                .add("p", "string")))))))
    }
  }

  ddlTest("ADD COLUMNS into Map should fail if key or value not specified") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), struct("v2")))) { tableName =>

      val ex = intercept[AnalysisException] {
        sql(
          s"""
             |ALTER TABLE $tableName ADD COLUMNS (m.mkv3 long)
           """.stripMargin)
      }
      checkErrMsg(ex.getMessage, Seq("m", "mkv3"))
    }
  }

  ddlTest("ADD COLUMNS into Array should fail if element is not specified") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("a", array(struct("v1")))) { tableName =>

      intercept[AnalysisException] {
        sql(
          s"""
             |ALTER TABLE $tableName ADD COLUMNS (a.v3 long)
         """.stripMargin)
      }
    }
  }

  ddlTest("ADD COLUMNS - a partitioned table") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), Seq("v2")) { tableName =>

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long, v4 double)")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long").add("v4", "double"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, Option[Long], Option[Double])],
        (1, "a", None, None), (2, "b", None, None))
    }
  }

  ddlTest("ADD COLUMNS - with a comment") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long COMMENT 'new column')")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long", true, "new column"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, Option[Long])],
        (1, "a", None), (2, "b", None))
    }
  }

  ddlTest("ADD COLUMNS - adding to a non-struct column") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (v2.x long)")
      }
      checkErrMsg(ex.getMessage, Seq("v2", "x"))
    }
  }

  ddlTest("ADD COLUMNS - a duplicate name") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (v2 long)")
      }
    }
  }

  ddlTest("ADD COLUMNS - a duplicate name (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (struct.v2 long)")
      }
    }
  }

  ddlTest("ADD COLUMNS - column name with spaces") {
    if (!columnMappingEnabled) {
      withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>
        val ex = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName ADD COLUMNS (`a column name with spaces` long)")
        }
        assert(ex.getMessage.contains("invalid character(s)"))
      }
    } else {
      // column mapping mode supports arbitrary column names
      withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>
        sql(s"ALTER TABLE $tableName ADD COLUMNS (`a column name with spaces` long)")
      }
    }
  }

  ddlTest("ADD COLUMNS - column name with spaces (nested)") {
    if (!columnMappingEnabled) {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
      withDeltaTable(df) { tableName =>
        val ex = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName ADD COLUMNS (struct.`a column name with spaces` long)")
        }
        assert(ex.getMessage.contains("invalid character(s)"))
      }
    } else {
      // column mapping mode supports arbitrary column names
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
      withDeltaTable(df) { tableName =>
        sql(s"ALTER TABLE $tableName ADD COLUMNS (struct.`a column name with spaces` long)")
      }
    }
  }

  ddlTest("ADD COLUMNS - special column names") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("z.z", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (`x.x` long, `z.z`.`y.y` double)")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("z.z", new StructType()
          .add("v1", "integer").add("v2", "string").add("y.y", "double"))
        .add("x.x", "long"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (Int, String, Option[Double]), Option[Long])],
        (1, "a", (1, "a", None), None), (2, "b", (2, "b", None), None))
    }
  }

  test("ADD COLUMNS - with positions") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withDeltaTable(df) { tableName =>

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long FIRST, v4 long AFTER v1, v5 long)")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v3", "long").add("v1", "integer")
        .add("v4", "long").add("v2", "string").add("v5", "long"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Option[Long], Int, Option[Long], String, Option[Long])],
        (None, 1, None, "a", None), (None, 2, None, "b", None))
    }
  }

  test("ADD COLUMNS - with positions using an added column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withDeltaTable(df) { tableName =>

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String)],
        (1, "a"), (2, "b"))

      sql("ALTER TABLE delta_test ADD COLUMNS (v3 long FIRST, v4 long AFTER v3, v5 long AFTER v4)")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v3", "long").add("v4", "long").add("v5", "long")
        .add("v1", "integer").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Option[Long], Option[Long], Option[Long], Int, String)],
        (None, None, None, 1, "a"), (None, None, None, 2, "b"))
    }
  }

  test("ADD COLUMNS - nested columns") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))

      sql("ALTER TABLE delta_test ADD COLUMNS " +
        "(struct.v3 long FIRST, struct.v4 long AFTER v1, struct.v5 long)")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v3", "long").add("v1", "integer")
          .add("v4", "long").add("v2", "string").add("v5", "long")))

      checkDatasetUnorderly(
        spark.table("delta_test")
          .as[(Int, String, (Option[Long], Int, Option[Long], String, Option[Long]))],
        (1, "a", (None, 1, None, "a", None)), (2, "b", (None, 2, None, "b", None)))
    }
  }

  test("ADD COLUMNS - special column names with positions") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("z.z", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (`x.x` long after v1, `z.z`.`y.y` double)")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("x.x", "long").add("v2", "string")
        .add("z.z", new StructType()
          .add("v1", "integer").add("v2", "string").add("y.y", "double"))
      )

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, Option[Long], String, (Int, String, Option[Double]))],
        (1, None, "a", (1, "a", None)), (2, None, "b", (2, "b", None)))
    }
  }

  test("ADD COLUMNS - adding after an unknown column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withDeltaTable(df) { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long AFTER unknown)")
      }
      assert(
        ex.getMessage.contains("Couldn't find") || ex.getMessage.contains("No such struct field"))
    }
  }

  test("ADD COLUMNS - case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      withDeltaTable(df) { tableName =>

        sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long AFTER V1)")

        val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
        assertEqual(snapshot.schema, new StructType()
          .add("v1", "integer").add("v3", "long").add("v2", "string"))
      }
    }
  }

  test("ADD COLUMNS - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      withDeltaTable(df) { tableName =>

        val ex = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long AFTER V1)")
        }
        assert(
          ex.getMessage.contains("Couldn't find") || ex.getMessage.contains("No such struct field"))
      }
    }
  }

  test("ADD COLUMNS - adding after an Array<MapType> column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("v3", array(map(col("v1"), col("v2"))))
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v4 string AFTER V3)")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", IntegerType)
        .add("v2", StringType)
        .add("v3", ArrayType(
          MapType(IntegerType, StringType)))
        .add("v4", StringType))
    }
  }

  ///////////////////////////////
  // CHANGE COLUMN
  ///////////////////////////////

  ddlTest("CHANGE COLUMN - add a comment") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer COMMENT 'a comment'")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer", true, "a comment").add("v2", "string"))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to a partitioned table") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2"), Seq("v2")) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v2 v2 string COMMENT 'a comment'")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string", true, "a comment"))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to special column names (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("x.x", "y.y")
      .withColumn("z.z", struct("`x.x`", "`y.y`"))
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN `z.z`.`x.x` `x.x` integer COMMENT 'a comment'")
      sql(s"ALTER TABLE $tableName CHANGE COLUMN `x.x` `x.x` integer COMMENT 'another comment'")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("x.x", "integer", true, "another comment")
        .add("y.y", "string")
        .add("z.z", new StructType()
          .add("x.x", "integer", true, "a comment").add("y.y", "string")))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to a MapType (nested)") {
    val table = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("a", array(struct(array(struct(map(struct("v1"), struct("v2")))))))
    withDeltaTable(table) { tableName =>
      sql(
        s"""
           |ALTER TABLE $tableName CHANGE COLUMN
           |a.element.col1.element.col1 col1 MAP<STRUCT<v1:int>,
           |STRUCT<v2:string>> COMMENT 'a comment'
         """.stripMargin)

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(new StructType()
            .add("col1", ArrayType(new StructType()
              .add("col1", MapType(
                new StructType()
                  .add("v1", "integer"),
                new StructType()
                  .add("v2", "string")), nullable = true, "a comment"))))))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to an ArrayType (nested)") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("m", map(struct("v1"), struct(array(struct(struct("v1"))))))) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName CHANGE COLUMN
           |m.value.col1.element.col1.v1 v1 integer COMMENT 'a comment'
         """.stripMargin)

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer"),
          new StructType()
            .add("col1", ArrayType(new StructType()
              .add("col1", new StructType()
                .add("v1", "integer", nullable = true, "a comment")))))))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to an ArrayType") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("a", array('v1))) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN a a ARRAY<int> COMMENT 'a comment'")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", ArrayType(IntegerType), nullable = true, "a comment"))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment to a MapType") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("a", map('v1, 'v2))) { tableName =>

      sql(
        s"""
           |ALTER TABLE $tableName CHANGE COLUMN
           |a a MAP<int, string> COMMENT 'a comment'
         """.stripMargin)

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("a", MapType(IntegerType, StringType), nullable = true, "a comment"))
    }
  }

  ddlTest("CHANGE COLUMN - (unsupported) add a comment to key/value of a MapType") {
    val df = Seq((1, 1), (2, 2)).toDF("v1", "v2")
      .withColumn("a", map('v1, 'v2))
    withDeltaTable(df) { tableName =>
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN a.key COMMENT 'a comment'")
        },
        "DELTA_UNSUPPORTED_COMMENT_MAP_ARRAY",
        parameters = Map("fieldPath" -> "a.key")
      )
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN a.value COMMENT 'a comment'")
        },
        "DELTA_UNSUPPORTED_COMMENT_MAP_ARRAY",
        parameters = Map("fieldPath" -> "a.value")
      )
    }
  }

  ddlTest("CHANGE COLUMN - (unsupported) add a comment to element of an array") {
    val df = Seq(1, 2).toDF("v1")
      .withColumn("a", array('v1))
    withDeltaTable(df) { tableName =>
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN a.element COMMENT 'a comment'")
        },
        "DELTA_UNSUPPORTED_COMMENT_MAP_ARRAY",
        parameters = Map("fieldPath" -> "a.element")
      )
    }
  }

  ddlTest("RENAME COLUMN - (unsupported) rename key/value of a MapType") {
    val df = Seq((1, 1), (2, 2)).toDF("v1", "v2")
      .withColumn("a", map('v1, 'v2))
    withDeltaTable(df) { tableName =>
      checkError(
        intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName RENAME COLUMN a.key TO key2")
        },
        "INVALID_FIELD_NAME",
        parameters = Map(
          "fieldName" -> "`a`.`key2`",
          "path" -> "`a`"
        )
      )
      checkError(
        intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName RENAME COLUMN a.value TO value2")
        },
        "INVALID_FIELD_NAME",
        parameters = Map(
          "fieldName" -> "`a`.`value2`",
          "path" -> "`a`"
        )
      )
    }
  }

  ddlTest("RENAME COLUMN - (unsupported) rename element of an array") {
    val df = Seq(1, 2).toDF("v1")
      .withColumn("a", array('v1))
    withDeltaTable(df) { tableName =>
      checkError(
        intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName RENAME COLUMN a.element TO element2")
        },
        "INVALID_FIELD_NAME",
        parameters = Map(
          "fieldName" -> "`a`.`element2`",
          "path" -> "`a`"
        )
      )
    }
  }

  ddlTest("CHANGE COLUMN - change name") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>

      assertNotSupported(s"ALTER TABLE $tableName CHANGE COLUMN v2 v3 string")
    }
  }

  ddlTest("CHANGE COLUMN - incompatible") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 long")
        },
        "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
        parameters = Map(
          "fieldPath" -> "v1",
          "oldField" -> "INT",
          "newField" -> "BIGINT"
        )
      )
    }
  }

  ddlTest("CHANGE COLUMN - incompatible (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 long")
        },
        "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
        parameters = Map(
          "fieldPath" -> "struct.v1",
          "oldField" -> "INT",
          "newField" -> "BIGINT"
        )
      )
    }
  }

  ddlTest("CHANGE COLUMN - (unsupported) change type of key of a MapType") {
    val df = Seq((1, 1), (2, 2)).toDF("v1", "v2")
      .withColumn("a", map('v1, 'v2))
    withDeltaTable(df) { tableName =>
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN a.key key long")
        },
        "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
        parameters = Map(
          "fieldPath" -> "a.key",
          "oldField" -> "INT NOT NULL",
          "newField" -> "BIGINT NOT NULL"
        )
      )
    }
  }

  ddlTest("CHANGE COLUMN - (unsupported) change type of value of a MapType") {
    val df = Seq((1, 1), (2, 2)).toDF("v1", "v2")
      .withColumn("a", map('v1, 'v2))
    withDeltaTable(df) { tableName =>
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN a.value value long")
        },
        "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
        parameters = Map(
          "fieldPath" -> "a.value",
          "oldField" -> "INT",
          "newField" -> "BIGINT"
        )
      )
    }
  }

  ddlTest("CHANGE COLUMN - (unsupported) change type of element of an ArrayType") {
    val df = Seq(1).toDF("v1")
      .withColumn("a", array('v1))
    withDeltaTable(df) { tableName =>
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN a.element element long")
        },
        "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
        parameters = Map(
          "fieldPath" -> "a.element",
          "oldField" -> "INT",
          "newField" -> "BIGINT"
        )
      )
    }
  }

  test("CHANGE COLUMN - move to first") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v2 v2 string FIRST")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v2", "string").add("v1", "integer"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(String, Int)],
        ("a", 1), ("b", 2))
    }
  }

  test("CHANGE COLUMN - move to first (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v2 v2 string FIRST")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v2", "string").add("v1", "integer")))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (String, Int))],
        (1, "a", ("a", 1)), (2, "b", ("b", 2)))

      // Can't change the inner ordering
      assertNotSupported(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v2:string> FIRST")

      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v2:string, v1:integer> FIRST")

      assertEqual(deltaLog.update().schema, new StructType()
        .add("struct", new StructType().add("v2", "string").add("v1", "integer"))
        .add("v1", "integer").add("v2", "string"))
    }
  }

  test("CHANGE COLUMN - move a partitioned column to first") {
    val df = Seq((1, "a", true), (2, "b", false)).toDF("v1", "v2", "v3")
    withDeltaTable(df, Seq("v2", "v3")) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v3 v3 boolean FIRST")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v3", "boolean").add("v1", "integer").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Boolean, Int, String)],
        (true, 1, "a"), (false, 2, "b"))
    }
  }

  test("CHANGE COLUMN - move to after some column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER v2")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v2", "string").add("v1", "integer"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(String, Int)],
        ("a", 1), ("b", 2))
    }
  }

  test("CHANGE COLUMN - move to after some column (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 integer AFTER v2")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v2", "string").add("v1", "integer")))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (String, Int))],
        (1, "a", ("a", 1)), (2, "b", ("b", 2)))

      // cannot change ordering within the struct
      assertNotSupported(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v2:string> AFTER v1")

      // can move the struct itself however
      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v2:string, v1:integer> AFTER v1")

      assertEqual(deltaLog.update().schema, new StructType()
        .add("v1", "integer")
        .add("struct", new StructType().add("v2", "string").add("v1", "integer"))
        .add("v2", "string"))
    }
  }

  test("CHANGE COLUMN - move to after the same column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER v1")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))
    }
  }

  test("CHANGE COLUMN - move to after the same column (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 integer AFTER v1")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v1", "integer").add("v2", "string")))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))
    }
  }

  test("CHANGE COLUMN - move a partitioned column to after some column") {
    val df = Seq((1, "a", true), (2, "b", false)).toDF("v1", "v2", "v3")
    withDeltaTable(df, Seq("v2", "v3")) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v3 v3 boolean AFTER v1")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v3", "boolean").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, Boolean, String)],
        (1, true, "a"), (2, false, "b"))
    }
  }

  test("CHANGE COLUMN - move to after the last column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER v2")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v2", "string").add("v1", "integer"))
    }
  }

  test("CHANGE COLUMN - special column names with positions") {
    val df = Seq((1, "a"), (2, "b")).toDF("x.x", "y.y")
    withDeltaTable(df) { tableName =>
      sql(s"ALTER TABLE $tableName CHANGE COLUMN `x.x` `x.x` integer AFTER `y.y`")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("y.y", "string").add("x.x", "integer"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(String, Int)],
        ("a", 1), ("b", 2))
    }
  }

  test("CHANGE COLUMN - special column names (nested) with positions") {
    val df = Seq((1, "a"), (2, "b")).toDF("x.x", "y.y")
      .withColumn("z.z", struct("`x.x`", "`y.y`"))
    withDeltaTable(df) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN `z.z`.`x.x` `x.x` integer AFTER `y.y`")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("x.x", "integer").add("y.y", "string")
        .add("z.z", new StructType()
          .add("y.y", "string").add("x.x", "integer")))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, (String, Int))],
        (1, "a", ("a", 1)), (2, "b", ("b", 2)))
    }
  }

  test("CHANGE COLUMN - move to after an unknown column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withDeltaTable(df) { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER unknown")
      }
      checkExceptionMessage(
        ex,
        "Missing field unknown",
        "Couldn't resolve positional argument AFTER unknown",
        "A column, variable, or function parameter with name `unknown` cannot be resolved")
    }
  }

  test("CHANGE COLUMN - move to after an unknown column (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 integer AFTER unknown")
      }
      checkExceptionMessage(
        ex,
        "Missing field struct.unknown",
        "Couldn't resolve positional argument AFTER unknown",
        "A column, variable, or function parameter with name `struct`.`unknown` cannot be resolved")
    }
  }

  test("CHANGE COLUMN - complex types nullability tests") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withDeltaTable(df) { tableName =>
      // not supported to tighten nullabilities.
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN s s STRUCT<v1:int, v2:string NOT NULL>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN a a " +
          "ARRAY<STRUCT<v1:int, v2:string NOT NULL>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string NOT NULL>, STRUCT<v1:int, v2:string>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string NOT NULL>>")

      // not supported to add not-null columns.
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN s s " +
          "STRUCT<v1:int, v2:string, sv3:long, sv4:long NOT NULL>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN a a " +
          "ARRAY<STRUCT<v1:int, v2:string, av3:long, av4:long NOT NULL>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string, mkv3:long, mkv4:long NOT NULL>, " +
          "STRUCT<v1:int, v2:string, mvv3:long>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string, mkv3:long>, " +
          "STRUCT<v1:int, v2:string, mvv3:long, mvv4:long NOT NULL>>")
    }
  }

  test("CHANGE COLUMN - (unsupported) change nullability of map key/value and array element") {
    val df = Seq((1, 1), (2, 2))
      .toDF("key", "value")
      .withColumn("m", map(col("key"), col("value")))
      .withColumn("a", array(col("value")))

    withDeltaTable(df) { tableName =>
      val schema = spark.read.table(tableName).schema
      assert(schema("m").dataType ===
        MapType(IntegerType, IntegerType, valueContainsNull = true))
      assert(schema("a").dataType ===
        ArrayType(IntegerType, containsNull = true))

      // No-op actions are allowed - map keys are always non-nullable.
      sql(s"ALTER TABLE $tableName CHANGE COLUMN m.key SET NOT NULL")
      sql(s"ALTER TABLE $tableName CHANGE COLUMN m.value DROP NOT NULL")
      sql(s"ALTER TABLE $tableName CHANGE COLUMN a.element DROP NOT NULL")

      // Changing the nullability of map/array fields is not allowed.
      var statement = s"ALTER TABLE $tableName CHANGE COLUMN m.key DROP NOT NULL"
      checkError(
        intercept[AnalysisException] { sql(statement) },
        "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
        parameters = Map(
          "fieldPath" -> "m.key",
          "oldField" -> "INT NOT NULL",
          "newField" -> "INT"
        )
      )

      statement = s"ALTER TABLE $tableName CHANGE COLUMN m.value SET NOT NULL"
      checkError(
        intercept[AnalysisException] { sql(statement) },
        "_LEGACY_ERROR_TEMP_2330",
        parameters = Map(
          "fieldName" -> "m.value"
        ),
        context = ExpectedContext(statement, 0, statement.length - 1)
      )

      statement = s"ALTER TABLE $tableName CHANGE COLUMN a.element SET NOT NULL"
      checkError(
        intercept[AnalysisException] { sql(statement) },
        "_LEGACY_ERROR_TEMP_2330",
        parameters = Map(
          "fieldName" -> "a.element"
        ),
        context = ExpectedContext(statement, 0, statement.length - 1)
      )
    }
  }

  ddlTest("CHANGE COLUMN - set comment on a varchar column") {
    withDeltaTable(schema = "v varchar(1)") { tableName =>
      sql(s"ALTER TABLE $tableName CHANGE COLUMN v COMMENT 'test comment'")
      val expectedResult = Row("v", "string", "test comment") :: Nil
      checkAnswer(
        sql(s"DESCRIBE $tableName").filter("col_name = 'v'"),
        expectedResult)
      checkColType(spark.table(tableName).schema.head, VarcharType(1))
      val e = intercept[DeltaInvariantViolationException] {
        sql(s"INSERT into $tableName values ('12')")
      }
      assert(e.getMessage.contains("Value \"12\" exceeds char/varchar type length limitation. " +
        "Failed check: ((v IS NULL) OR (length(v) <= 1))"))
    }
  }

  ddlTest("CHANGE COLUMN - set comment on a char column") {
    withDeltaTable(schema = "v char(1)") { tableName =>
      sql(s"ALTER TABLE $tableName CHANGE COLUMN v COMMENT 'test comment'")
      val expectedResult = Row("v", "string", "test comment") :: Nil
      checkAnswer(
        sql(s"DESCRIBE $tableName").filter("col_name = 'v'"),
        expectedResult)
      checkColType(spark.table(tableName).schema.head, CharType(1))
    }
  }

  ddlTest("CHANGE COLUMN - set comment on a array/map/struct<varchar> column") {
    val schema = """
      |arr_c array<char(1)>,
      |map_cc map<char(1), char(1)>,
      |map_sc map<string, char(1)>,
      |map_cs map<char(1), string>,
      |struct_c struct<v: char(1)>,
      |arr_v array<varchar(1)>,
      |map_vv map<varchar(1), varchar(1)>,
      |map_sv map<string, varchar(1)>,
      |map_vs map<varchar(1), string>,
      |struct_v struct<v: varchar(1)>""".stripMargin
    def testCommentOnVarcharInContainer(
      colName: String,
      expectedType: String,
      goodInsertValue: String,
      badInsertValue: String
    ): Unit = {
      withDeltaTable(schema = schema) { tableName =>
        sql(s"ALTER TABLE $tableName CHANGE COLUMN $colName COMMENT 'test comment'")
        val expectedResult = Row(colName, expectedType, "test comment") :: Nil
        checkAnswer(
          sql(s"DESCRIBE $tableName").filter(s"col_name = '$colName'"),
          expectedResult)
        sql(s"INSERT into $tableName($colName) values ($goodInsertValue)")
        val e = intercept[DeltaInvariantViolationException] {
          sql(s"INSERT into $tableName($colName) values ($badInsertValue)")
        }
        assert(e.getMessage.contains("exceeds char/varchar type length limitation"))
      }
    }
    testCommentOnVarcharInContainer(
      colName = "arr_c",
      expectedType = "array<string>",
      goodInsertValue = "array('1')",
      badInsertValue = "array('12')")
    testCommentOnVarcharInContainer(
      colName = "map_cc",
      expectedType = "map<string,string>",
      goodInsertValue = "map('1', '1')",
      badInsertValue = "map('12', '12')")
    testCommentOnVarcharInContainer(
      colName = "map_sc",
      expectedType = "map<string,string>",
      goodInsertValue = "map('123', '1')",
      badInsertValue = "map('123', '12')")
    testCommentOnVarcharInContainer(
      colName = "map_cs",
      expectedType = "map<string,string>",
      goodInsertValue = "map('1', '123')",
      badInsertValue = "map('12', '123')")
    testCommentOnVarcharInContainer(
      colName = "struct_c",
      expectedType = "struct<v:string>",
      goodInsertValue = "named_struct('v', '1')",
      badInsertValue = "named_struct('v', '12')")
    testCommentOnVarcharInContainer(
      colName = "arr_v",
      expectedType = "array<string>",
      goodInsertValue = "array('1')",
      badInsertValue = "array('12')")
    testCommentOnVarcharInContainer(
      colName = "map_vv",
      expectedType = "map<string,string>",
      goodInsertValue = "map('1', '1')",
      badInsertValue = "map('12', '12')")
    testCommentOnVarcharInContainer(
      colName = "map_sv",
      expectedType = "map<string,string>",
      goodInsertValue = "map('123', '1')",
      badInsertValue = "map('123', '12')")
    testCommentOnVarcharInContainer(
      colName = "map_vs",
      expectedType = "map<string,string>",
      goodInsertValue = "map('1', '123')",
      badInsertValue = "map('12', '123')")
    testCommentOnVarcharInContainer(
      colName = "struct_v",
      expectedType = "struct<v:string>",
      goodInsertValue = "named_struct('v', '1')",
      badInsertValue = "named_struct('v', '12')")
  }

  ddlTest("CHANGE COLUMN - set a default value for a varchar column") {
    withDeltaTable(schema = "v varchar(1)") { tableName =>
      sql(s"ALTER TABLE $tableName " +
        s"SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
      sql(s"ALTER TABLE $tableName CHANGE COLUMN v set default cast('a' as varchar(1))")
      val expectedResult = Row("v", "string", null) :: Nil
      checkAnswer(
        sql(s"DESCRIBE $tableName").filter("col_name = 'v'"),
        expectedResult)
      checkColType(spark.table(tableName).schema.head, VarcharType(1))
    }
  }

  ddlTest("CHANGE COLUMN - change name (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>

      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN struct.v2 v3 string")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
          "STRUCT<v1:integer, v3:string>")
    }
  }

  ddlTest("CHANGE COLUMN - add a comment (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>
      sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.v1 v1 integer COMMENT 'a comment'")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v1", "integer", true, "a comment").add("v2", "string")))

      assertNotSupported(s"ALTER TABLE $tableName CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v2:string COMMENT 'a comment for v2'>")
    }
  }

  ddlTest("CHANGE COLUMN - complex types not supported because behavior is ambiguous") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("s", struct("v1", "v2"))
      .withColumn("a", array("s"))
      .withColumn("m", map(col("s"), col("s")))
    withDeltaTable(df) { tableName =>
      // not supported to add columns
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN s s STRUCT<v1:int, v2:string, sv3:long>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN a a ARRAY<STRUCT<v1:int, v2:string, av3:long>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string, mkv3:long>, STRUCT<v1:int, v2:string, mvv3:long>>")

      // not supported to remove columns.
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN s s STRUCT<v1:int>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN a a ARRAY<STRUCT<v1:int>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int>, STRUCT<v1:int, v2:string>>")
      assertNotSupported(
        s"ALTER TABLE $tableName CHANGE COLUMN m m " +
          "MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int>>")
    }
  }

  private def checkExceptionMessage(e: AnalysisException, messages: String*): Unit = {
    assert(messages.exists(e.getMessage.contains), s"${e.getMessage} did not contain $messages")
  }

  test("CHANGE COLUMN - move unknown column") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
    withDeltaTable(df) { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName CHANGE COLUMN unknown unknown string FIRST")
      }
      checkExceptionMessage(
        ex,
        "Missing field unknown",
        "Cannot update missing field unknown",
        "A column, variable, or function parameter with name `unknown` cannot be resolved")
    }
  }

  test("CHANGE COLUMN - move unknown column (nested)") {
    val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
      .withColumn("struct", struct("v1", "v2"))
    withDeltaTable(df) { tableName =>

      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName CHANGE COLUMN struct.unknown unknown string FIRST")
      }
      checkExceptionMessage(
        ex,
        "Missing field struct.unknown",
        "Cannot update missing field struct.unknown",
        "A column, variable, or function parameter with name `struct`.`unknown` cannot be resolved")
    }
  }

  test("CHANGE COLUMN - case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
      withDeltaTable(df) { tableName =>

        val (deltaLog, _) = getDeltaLogWithSnapshot(tableName)

        sql(s"ALTER TABLE $tableName CHANGE COLUMN V1 v1 integer")

        assertEqual(deltaLog.update().schema, new StructType()
          .add("v1", "integer").add("v2", "string")
          .add("s", new StructType().add("v1", "integer").add("v2", "string")))

        sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 V1 integer")

        assertEqual(deltaLog.update().schema, new StructType()
          .add("v1", "integer").add("v2", "string")
          .add("s", new StructType().add("v1", "integer").add("v2", "string")))

        sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER V2")

        assertEqual(deltaLog.update().schema, new StructType()
          .add("v2", "string").add("v1", "integer")
          .add("s", new StructType().add("v1", "integer").add("v2", "string")))

        // Since the struct doesn't match the case this fails
        assertNotSupported(
          s"ALTER TABLE $tableName CHANGE COLUMN s s struct<V1:integer,v2:string> AFTER V2")

        sql(
          s"ALTER TABLE $tableName CHANGE COLUMN s s struct<v1:integer,v2:string> AFTER V2")

        assertEqual(deltaLog.update().schema, new StructType()
          .add("v2", "string")
          .add("s", new StructType().add("v1", "integer").add("v2", "string"))
          .add("v1", "integer"))
      }
    }
  }

  test("CHANGE COLUMN - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val df = Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
      withDeltaTable(df) { tableName =>

        val ex1 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN V1 V1 integer")
        }
        checkExceptionMessage(
          ex1,
          "Missing field V1",
          "Cannot update missing field V1",
          "A column, variable, or function parameter with name `V1` cannot be resolved.")

        val ex2 = intercept[ParseException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 V1 integer")
        }
        assert(ex2.getMessage.contains("Renaming column is not supported"))

        val ex3 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer AFTER V2")
        }
        checkExceptionMessage(
          ex2,
          "Missing field V2",
          "Couldn't resolve positional argument AFTER V2",
          "Renaming column is not supported in Hive-style ALTER COLUMN, " +
            "please run RENAME COLUMN instead")

        val ex4 = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN s s struct<V1:integer,v2:string> AFTER v2")
        }
        assert(ex4.getMessage.contains("Cannot update"))
      }
    }
  }

  test("CHANGE COLUMN: allow to change change column from char to string type") {
    withTable("t") {
      sql("CREATE TABLE t(i STRING, c CHAR(4)) USING delta")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE STRING")
      assert(spark.table("t").schema(1).dataType === StringType)
    }
  }

  test("CHANGE COLUMN: allow to change map key from char to string type") {
    withTable("t") {
      sql("CREATE TABLE t(i STRING, m map<CHAR(4), INT>) USING delta")
      sql("ALTER TABLE t CHANGE COLUMN m.key TYPE STRING")
      assert(spark.table("t").schema(1).dataType === MapType(StringType, IntegerType))
    }
  }

  test("CHANGE COLUMN: allow to change map value from char to string type") {
    withTable("t") {
      sql("CREATE TABLE t(i STRING, m map<INT, CHAR(4)>) USING delta")
      sql("ALTER TABLE t CHANGE COLUMN m.value TYPE STRING")
      assert(spark.table("t").schema(1).dataType === MapType(IntegerType, StringType))
    }
  }

  test("CHANGE COLUMN: allow to change array element from char to string type") {
    withTable("t") {
      sql("CREATE TABLE t(i STRING, a array<CHAR(4)>) USING delta")
      sql("ALTER TABLE t CHANGE COLUMN a.element TYPE STRING")
      assert(spark.table("t").schema(1).dataType === ArrayType(StringType))
    }
  }

  private def checkColType(f: StructField, dt: DataType): Unit = {
    assert(f.dataType == CharVarcharUtils.replaceCharVarcharWithString(dt))
    assert(CharVarcharUtils.getRawType(f.metadata).contains(dt))
  }

  test("CHANGE COLUMN: allow to change column from char(x) to varchar(y) type x <= y") {
    withTable("t") {
      sql("CREATE TABLE t(i STRING, c CHAR(4)) USING delta")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(4)")
      checkColType(spark.table("t").schema(1), VarcharType(4))
    }
    withTable("t") {
      sql("CREATE TABLE t(i STRING, c CHAR(4)) USING delta")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(5)")
      checkColType(spark.table("t").schema(1), VarcharType(5))
    }
  }

  test("CHANGE COLUMN: allow to change column from varchar(x) to varchar(y) type x <= y") {
    withTable("t") {
      sql("CREATE TABLE t(i STRING, c VARCHAR(4)) USING delta")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(4)")
      checkColType(spark.table("t").schema(1), VarcharType(4))
      sql("ALTER TABLE t CHANGE COLUMN c TYPE VARCHAR(5)")
      checkColType(spark.table("t").schema(1), VarcharType(5))
    }
  }

  for (charVarcharMitigationDisabled <- BOOLEAN_DOMAIN)
  test(s"CHANGE COLUMN: allow change from char(x) to string type " +
    s"[charVarcharMitigationDisabled: $charVarcharMitigationDisabled]") {
    withSQLConf(
      DeltaSQLConf.DELTA_BYPASS_CHARVARCHAR_TO_STRING_FIX.key ->
        charVarcharMitigationDisabled.toString) {
      withTable("t") {
        sql("CREATE TABLE t(i VARCHAR(4)) USING delta")
        sql("ALTER TABLE t CHANGE COLUMN i TYPE STRING")
        val col = spark.table("t").schema.head
        assert(col.dataType == StringType)
        assert(CharVarcharUtils.getRawType(col.metadata).isDefined == charVarcharMitigationDisabled)
        if (!charVarcharMitigationDisabled) {
          sql("INSERT INTO t VALUES ('123456789')")
        }
      }
    }
  }
}

trait DeltaAlterTableByNameTests extends DeltaAlterTableTests {
  import testImplicits._

  override protected def createTable(schema: String, tblProperties: Map[String, String]): String = {
    val props = tblProperties.map { case (key, value) =>
      s"'$key' = '$value'"
    }.mkString(", ")
    val propsString = if (tblProperties.isEmpty) "" else s" TBLPROPERTIES ($props)"
    sql(s"CREATE TABLE delta_test ($schema) USING delta$propsString")
    "delta_test"
  }

  override protected def createTable(df: DataFrame, partitionedBy: Seq[String]): String = {
    df.write.partitionBy(partitionedBy: _*).format("delta").saveAsTable("delta_test")
    "delta_test"
  }

  override protected def dropTable(identifier: String): Unit = {
    sql(s"DROP TABLE IF EXISTS $identifier")
  }

  override protected def getDeltaLogWithSnapshot(identifier: String): (DeltaLog, Snapshot) = {
    DeltaLog.forTableWithSnapshot(spark, TableIdentifier(identifier))
  }

  test("ADD COLUMNS - external table") {
    withTempDir { dir =>
      withTable("delta_test") {
        val path = dir.getCanonicalPath
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .write
          .format("delta")
          .option("path", path)
          .saveAsTable("delta_test")

        checkDatasetUnorderly(
          spark.table("delta_test").as[(Int, String)],
          (1, "a"), (2, "b"))

        sql("ALTER TABLE delta_test ADD COLUMNS (v3 long, v4 double)")

        val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, path)
        assertEqual(snapshot.schema, new StructType()
          .add("v1", "integer").add("v2", "string")
          .add("v3", "long").add("v4", "double"))

        checkDatasetUnorderly(
          spark.table("delta_test").as[(Int, String, Option[Long], Option[Double])],
          (1, "a", None, None), (2, "b", None, None))
        checkDatasetUnorderly(
          spark.read.format("delta").load(path).as[(Int, String, Option[Long], Option[Double])],
          (1, "a", None, None), (2, "b", None, None))
      }
    }
  }

  // LOCATION tests do not make sense for by path access
  testQuietly("SET LOCATION") {
    withTable("delta_table") {
      spark.range(1).write.format("delta").saveAsTable("delta_table")
      val catalog = spark.sessionState.catalog
      val table = catalog.getTableMetadata(TableIdentifier(tableName = "delta_table"))
      val oldLocation = table.location.toString
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        spark.range(1, 2).write.format("delta").save(path)
        checkAnswer(spark.table("delta_table"), Seq(Row(0)))
        sql(s"alter table delta_table set location '$path'")
        checkAnswer(spark.table("delta_table"), Seq(Row(1)))
      }
      Utils.deleteRecursively(new File(oldLocation.stripPrefix("file:")))
    }
  }

  testQuietly("SET LOCATION: external delta table") {
    withTable("delta_table") {
      withTempDir { oldDir =>
        spark.range(1).write.format("delta").save(oldDir.getCanonicalPath)
        sql(s"CREATE TABLE delta_table USING delta LOCATION '${oldDir.getCanonicalPath}'")
        withTempDir { dir =>
          val path = dir.getCanonicalPath
          spark.range(1, 2).write.format("delta").save(path)
          checkAnswer(spark.table("delta_table"), Seq(Row(0)))
          sql(s"alter table delta_table set location '$path'")
          checkAnswer(spark.table("delta_table"), Seq(Row(1)))
        }
      }
    }
  }

  test(
      "SET LOCATION - negative cases") {
    withTable("delta_table") {
      spark.range(1).write.format("delta").saveAsTable("delta_table")
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val catalog = spark.sessionState.catalog
        val table = catalog.getTableMetadata(TableIdentifier(tableName = "delta_table"))
        val oldLocation = table.location.toString

        // new location is not a delta table
        var e = intercept[AnalysisException] {
          sql(s"alter table delta_table set location '$path'")
        }
        assert(e.getMessage.contains("not a Delta table"))

        Seq("1").toDF("id").write.format("delta").save(path)

        // set location on specific partitions
        e = intercept[AnalysisException] {
          sql(s"alter table delta_table partition (id = 1) set location '$path'")
        }
        assert(Seq("partition", "not support").forall(e.getMessage.contains))

        // schema mismatch
        e = intercept[AnalysisException] {
          sql(s"alter table delta_table set location '$path'")
        }
        assert(e.getMessage.contains("different than the current table schema"))

        withSQLConf(DeltaSQLConf.DELTA_ALTER_LOCATION_BYPASS_SCHEMA_CHECK.key -> "true") {
          checkAnswer(spark.table("delta_table"), Seq(Row(0)))
          // now we can bypass the schema mismatch check
          sql(s"alter table delta_table set location '$path'")
          checkAnswer(spark.table("delta_table"), Seq(Row("1")))
        }
        Utils.deleteRecursively(new File(oldLocation.stripPrefix("file:")))
      }
    }
  }
}

/**
 * For ByPath tests, we select a test case per ALTER TABLE command to simply test identifier
 * resolution.
 */
trait DeltaAlterTableByPathTests extends DeltaAlterTableTestBase {
  override protected def createTable(schema: String, tblProperties: Map[String, String]): String = {
      val tmpDir = Utils.createTempDir().getCanonicalPath
      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tmpDir)
      // This is a path-based table so we don't need to pass the catalogTable here
      val txn = deltaLog.startTransaction(None, Some(snapshot))
      val metadata = Metadata(
        schemaString = StructType.fromDDL(schema).json,
        configuration = tblProperties)
      txn.commit(metadata :: Nil, DeltaOperations.ManualUpdate)
      s"delta.`$tmpDir`"
  }

  override protected def createTable(df: DataFrame, partitionedBy: Seq[String]): String = {
    val tmpDir = Utils.createTempDir().getCanonicalPath
    df.write.format("delta").partitionBy(partitionedBy: _*).save(tmpDir)
    s"delta.`$tmpDir`"
  }

  override protected def dropTable(identifier: String): Unit = {
    Utils.deleteRecursively(new File(identifier.stripPrefix("delta.`").stripSuffix("`")))
  }

  override protected def getDeltaLogWithSnapshot(identifier: String): (DeltaLog, Snapshot) = {
    DeltaLog.forTableWithSnapshot(spark, identifier.stripPrefix("delta.`").stripSuffix("`"))
  }

  override protected def ddlTest(testName: String)(f: => Unit): Unit = {
    super.ddlTest(testName)(f)

    testQuietly(testName + " with delta database") {
      withDatabase("delta") {
        spark.sql("CREATE DATABASE delta")
        f
      }
    }
  }

  import testImplicits._

  ddlTest("SET/UNSET TBLPROPERTIES - simple") {
    withDeltaTable("v1 int, v2 string") { tableName =>

      sql(s"""
             |ALTER TABLE $tableName
             |SET TBLPROPERTIES (
             |  'delta.logRetentionDuration' = '2 weeks',
             |  'delta.checkpointInterval' = '20',
             |  'key' = 'value'
             |)""".stripMargin)

      val (deltaLog, snapshot1) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot1.metadata.configuration, Map(
        "delta.logRetentionDuration" -> "2 weeks",
        "delta.checkpointInterval" -> "20",
        "key" -> "value"))
      assert(deltaLog.deltaRetentionMillis(snapshot1.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot1.metadata) == 20)

      sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('delta.checkpointInterval', 'key')")

      val snapshot2 = deltaLog.update()
      assertEqual(snapshot2.metadata.configuration,
        Map("delta.logRetentionDuration" -> "2 weeks"))
      assert(deltaLog.deltaRetentionMillis(snapshot2.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot2.metadata) ==
        CHECKPOINT_INTERVAL.fromString(CHECKPOINT_INTERVAL.defaultValue))
    }
  }

  ddlTest("ADD COLUMNS - simple") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>
      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String)],
        (1, "a"), (2, "b"))

      sql(s"ALTER TABLE $tableName ADD COLUMNS (v3 long, v4 double)")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long").add("v4", "double"))

      checkDatasetUnorderly(
        spark.table(tableName).as[(Int, String, Option[Long], Option[Double])],
        (1, "a", None, None), (2, "b", None, None))
    }
  }

  ddlTest("CHANGE COLUMN - add a comment") {
    withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>

      sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer COMMENT 'a comment'")

      val (deltaLog, snapshot) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot.schema, new StructType()
        .add("v1", "integer", true, "a comment").add("v2", "string"))
    }
  }

  test("SET LOCATION is not supported for path based tables") {
    val df = spark.range(1).toDF()
    withDeltaTable(df) { identifier =>
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val e = intercept[DeltaAnalysisException] {
          sql(s"alter table $identifier set location '$path'")
        }
        assert(e.getErrorClass == "DELTA_CANNOT_SET_LOCATION_ON_PATH_IDENTIFIER")
        assert(e.getSqlState == "42613")
        assert(e.getMessage == "[DELTA_CANNOT_SET_LOCATION_ON_PATH_IDENTIFIER] " +
          "Cannot change the location of a path based table.")
      }
    }
  }
}

class DeltaAlterTableByNameSuite
  extends DeltaAlterTableByNameTests
  with DeltaSQLCommandTest {

  ddlTest("SET/UNSET TBLPROPERTIES - unset non-existent config value should still" +
    "unset the config if key matches") {
    val props = Map(
      "delta.randomizeFilePrefixes" -> "true",
      "delta.randomPrefixLength" -> "5",
      "key" -> "value"
    )
    withDeltaTable("v1 int, v2 string", props) { tableName =>
      sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('delta.randomizeFilePrefixes', 'kEy')")

      val (deltaLog, snapshot1) = getDeltaLogWithSnapshot(tableName)
      assertEqual(snapshot1.metadata.configuration, Map(
        "delta.randomPrefixLength" -> "5",
        "key" -> "value"))

      sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES IF EXISTS " +
        "('delta.randomizeFilePrefixes', 'kEy')")

      val snapshot2 = deltaLog.update()
      assertEqual(snapshot2.metadata.configuration,
        Map("delta.randomPrefixLength" -> "5", "key" -> "value"))
    }
  }

}

class DeltaAlterTableByPathSuite extends DeltaAlterTableByPathTests with DeltaSQLCommandTest
  with DeltaAlterTableReplaceTests


trait DeltaAlterTableColumnMappingSelectedTests extends DeltaColumnMappingSelectedTestMixin {
  override protected def runOnlyTests = Seq(
    "ADD COLUMNS into complex types - Array",
    "CHANGE COLUMN - move to first (nested)",
    "CHANGE COLUMN - case insensitive")
}

class DeltaAlterTableByNameIdColumnMappingSuite extends DeltaAlterTableByNameSuite
  with DeltaColumnMappingEnableIdMode
  with DeltaAlterTableColumnMappingSelectedTests

class DeltaAlterTableByPathIdColumnMappingSuite extends DeltaAlterTableByPathSuite
  with DeltaColumnMappingEnableIdMode
  with DeltaAlterTableColumnMappingSelectedTests

class DeltaAlterTableByNameNameColumnMappingSuite extends DeltaAlterTableByNameSuite
  with DeltaColumnMappingEnableNameMode
  with DeltaAlterTableColumnMappingSelectedTests

class DeltaAlterTableByPathNameColumnMappingSuite extends DeltaAlterTableByPathSuite
  with DeltaColumnMappingEnableNameMode
  with DeltaAlterTableColumnMappingSelectedTests
