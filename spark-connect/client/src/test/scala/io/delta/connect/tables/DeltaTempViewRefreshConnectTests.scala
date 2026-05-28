/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.tables

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.DeltaQueryTest

/**
 * Section [1] tests: Temp views with stored plans (Connect behavior).
 * Temp views created from Dataset capture the plan. In Connect, they behave
 * the same as classic for the scenarios tested here.
 */
trait DeltaTempViewRefreshConnectTests {
  self: DeltaTableRefreshConnectTestBase with DeltaQueryTest with RemoteSparkSession =>

  // ---------------------------------------------------------------------------
  // Section [1]: Temp views with stored plans (Connect behavior)
  // Temp views created from Dataset capture the plan. In Connect, they behave
  // the same as classic for the scenarios tested here.
  // ---------------------------------------------------------------------------

  test("[1.1] connect: temp view picks up session writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] connect scenario 2: temp view with ADD COLUMN preserves original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // Same as classic: temp view preserves original schema (id, salary) but picks up new data
      checkAnswer(
        spark.sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] connect scenario 3: temp view after DROP COLUMN throws " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")

      // Same as classic: column mapping schema change throws
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  test("[1] connect scenario 7: temp view after ALTER COLUMN TYPE INT to BIGINT") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      // Same as classic: type change is detected as incompatible schema change.
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  test("[1] connect scenario 4: temp view after DROP and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // Same as classic without column mapping: resolves to new empty table
      checkAnswer(spark.sql("SELECT * FROM v"), Seq.empty)
    }
  }

  test("[1] connect scenario 4: temp view after DROP and recreate table " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      // Column IDs changed, so reading the view should fail.
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  test("[1] connect scenario 5: temp view after DROP/ADD column same name same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      // Same as classic: column mapping schema change (column IDs changed) throws
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  test("[1] connect scenario 6: temp view after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      // Same as classic: column mapping schema change throws
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  // ---------------------------------------------------------------------------
  // Section [1] external: Temp views with external modifications (Connect)
  // These test the "Connector w/ cache" behavior from the design doc.
  // ---------------------------------------------------------------------------

  test("[1] connect scenario 1 external: temp view with external data write") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("salary < 999").createOrReplaceTempView("v_1ext")
      checkAnswer(spark.sql("SELECT * FROM v_1ext"), Row(1, 100))

      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      checkAnswer(
        spark.sql("SELECT * FROM v_1ext ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] connect scenario 2 external: temp view with external ADD COLUMN") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("salary < 999").createOrReplaceTempView("v_2ext")
      checkAnswer(spark.sql("SELECT * FROM v_2ext"), Row(1, 100))

      writeExternalSchemaChangeCommitViaFilesystem(path, Seq((2, 200, -1)))

      // View preserves original schema (id, salary) but picks up new data
      checkAnswer(
        spark.sql("SELECT * FROM v_2ext ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] connect scenario 3 external: temp view with external DROP COLUMN " +
      "(column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id INT, salary INT) USING delta
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("id < 999").createOrReplaceTempView("v_3ext")
      checkAnswer(spark.sql("SELECT * FROM v_3ext"), Row(1, 100))

      writeExternalDropColumnViaFilesystem(path, "salary")

      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v_3ext").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] connect scenario 4 external: temp view after external DROP and recreate " +
      "(column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id INT, salary INT) USING delta
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("id < 999").createOrReplaceTempView("v_4ext")
      checkAnswer(spark.sql("SELECT * FROM v_4ext"), Row(1, 100))

      writeExternalDropAndRecreateColumnMappingViaFilesystem(path)

      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v_4ext").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] connect scenario 4 external: temp view after external DROP and recreate " +
      "(no column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("salary < 999").createOrReplaceTempView("v_4ext_nc")
      checkAnswer(spark.sql("SELECT * FROM v_4ext_nc"), Row(1, 100))

      writeExternalDropAndRecreateViaFilesystem(path)

      checkAnswer(spark.sql("SELECT * FROM v_4ext_nc"), Seq.empty)
    }
  }

  test("[1] connect scenario 5 external: temp view after external DROP/ADD column " +
      "same name same type (column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id INT, salary INT) USING delta
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("id < 999").createOrReplaceTempView("v_5ext")
      checkAnswer(spark.sql("SELECT * FROM v_5ext"), Row(1, 100))

      writeExternalReplaceColumnViaFilesystem(path, "salary")

      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v_5ext").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] connect scenario 6 external: temp view after external DROP/ADD column " +
      "same name different type (column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id INT, salary INT) USING delta
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("id < 999").createOrReplaceTempView("v_6ext")
      checkAnswer(spark.sql("SELECT * FROM v_6ext"), Row(1, 100))

      writeExternalReplaceColumnViaFilesystem(path, "salary", newType = Some("string"))

      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v_6ext").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }
}
