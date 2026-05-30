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

package io.delta.tables.shared

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.Row

/**
 * Section [1]: Temp views with stored plans.
 *
 * Shared across classic and Connect. Temp views backed by Delta tables handle data
 * changes, schema changes (ADD/DROP COLUMN, type widening), and table recreation.
 * Covers both session-initiated and external (DeltaLog-bypassing) mutations.
 * [[isConnect]] differentiates the few cases that diverge (classic STRICT mode).
 */
trait DeltaTempViewRefreshTests extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  // ---------------------------------------------------------------------------
  // Section [1]: Temp views with stored plans (session writes)
  // ---------------------------------------------------------------------------

  test("[1.1] temp view picks up writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Seq(Row(1, 100)))
      writerSql("INSERT INTO t VALUES (2, 200)")
      checkAnswer(spark.sql("SELECT * FROM v ORDER BY id"), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] scenario 2: temp view with ADD COLUMN preserves original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Seq(Row(1, 100)))
      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      if (v2EnableMode == "STRICT") {
        assertArityMismatchError { writerSql("INSERT INTO t VALUES (2, 200, -1)") }
      } else {
        writerSql("INSERT INTO t VALUES (2, 200, -1)")
        // View preserves original schema (id, salary) but picks up new data.
        checkAnswer(spark.sql("SELECT * FROM v ORDER BY id"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[1] scenario 3: temp view with DROP COLUMN throws (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      // STRICT's V2 connector reads column-mapping tables as empty over Connect.
      checkAnswer(spark.sql("SELECT * FROM v"), if (strictConnect) Seq.empty else Seq(Row(1, 100)))
      withColumnMappingDdl("ALTER TABLE t DROP COLUMN salary") {
        assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
      }
    }
  }

  test("[1] scenario 4: temp view after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), if (strictConnect) Seq.empty else Seq(Row(1, 100)))
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      if (v2EnableMode == "STRICT") {
        // After DSv2 migration, SQL view resolves tables by name; column IDs are not
        // captured, so drop/recreate reads empty.
        checkAnswer(spark.sql("SELECT * FROM v"), Seq.empty)
      } else {
        // Column IDs changed, so reading the view should fail.
        assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
      }
    }
  }

  test("[1] scenario 4: temp view after DROP and recreate table (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Seq(Row(1, 100)))
      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")
      // Without column mapping, no column ID check. New table is empty.
      checkAnswer(spark.sql("SELECT * FROM v"), Seq.empty)
    }
  }

  test("[1] scenario 5: temp view after DROP/ADD column same name same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), if (strictConnect) Seq.empty else Seq(Row(1, 100)))
      withColumnMappingDdl("ALTER TABLE t DROP COLUMN salary") {
        writerSql("ALTER TABLE t ADD COLUMN salary INT")
        if (!isConnect && v2EnableMode == "STRICT") {
          // After DSv2 migration, SQL views resolve columns by name, not column ID.
          checkAnswer(spark.sql("SELECT * FROM v"), Seq(Row(1, null)))
        } else {
          assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
        }
      }
    }
  }

  test("[1] scenario 6: temp view after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")
      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), if (strictConnect) Seq.empty else Seq(Row(1, 100)))
      withColumnMappingDdl("ALTER TABLE t DROP COLUMN salary") {
        writerSql("ALTER TABLE t ADD COLUMN salary STRING")
        assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
      }
    }
  }

  test("[1] scenario 7: temp view after ALTER COLUMN TYPE INT to BIGINT") {
    withTable("t") {
      createTypeWideningTable("t")
      insertInitialData("t")
      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), if (strictConnect) Seq.empty else Seq(Row(1, 100)))
      withColumnMappingDdl("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT") {
        assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Section [1] external: Temp views with external modifications.
  // These test the "Connector w/ cache" behavior from the design doc.
  // ---------------------------------------------------------------------------

  test("[1] scenario 1 external: temp view with external data write") {
    withRefreshTable { tableRef =>
      createSimpleTable(tableRef)
      insertInitialData(tableRef)
      spark.table(tableRef).filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Seq(Row(1, 100)))
      withExternalWrite(externalDataWrite(tableRef, Seq((2, 200)))) {
        checkAnswer(spark.sql("SELECT * FROM v ORDER BY id"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[1] scenario 2 external: temp view with external ADD COLUMN") {
    withRefreshTable { tableRef =>
      createSimpleTable(tableRef)
      insertInitialData(tableRef)
      spark.table(tableRef).filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Seq(Row(1, 100)))
      // View preserves original schema (id, salary) but picks up new data.
      withExternalWrite(externalAddColumnAndWrite(tableRef, Seq((2, 200, -1)))) {
        if (strictConnect) {
          // STRICT's V2 view path rejects the external column change.
          assertError("INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION", "View `v`") {
            spark.sql("SELECT * FROM v").collect()
          }
        } else {
          checkAnswer(spark.sql("SELECT * FROM v ORDER BY id"), Seq(Row(1, 100), Row(2, 200)))
        }
      }
    }
  }

  test("[1] scenario 3 external: temp view with external DROP COLUMN (column mapping)") {
    withRefreshTable { tableRef =>
      createColumnMappingTable(tableRef)
      insertInitialData(tableRef)
      spark.table(tableRef).filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), if (strictConnect) Seq.empty else Seq(Row(1, 100)))
      withExternalWrite(externalDropColumn(tableRef, "salary")) {
        if (strictConnect) {
          assertError("INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION", "View `v`") {
            spark.sql("SELECT * FROM v").collect()
          }
        } else assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
      }
    }
  }

  test("[1] scenario 4 external: temp view after external DROP and recreate " +
      "(column mapping)") {
    withRefreshTable { tableRef =>
      createColumnMappingTable(tableRef)
      insertInitialData(tableRef)
      spark.table(tableRef).filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), if (strictConnect) Seq.empty else Seq(Row(1, 100)))
      withExternalWrite(externalDropAndRecreate(tableRef, columnMapping = true)) {
        if (strictConnect) checkAnswer(spark.sql("SELECT * FROM v"), Seq.empty)
        else assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
      }
    }
  }

  test("[1] scenario 4 external: temp view after external DROP and recreate " +
      "(no column mapping)") {
    withRefreshTable { tableRef =>
      createSimpleTable(tableRef)
      insertInitialData(tableRef)
      spark.table(tableRef).filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Seq(Row(1, 100)))
      withExternalWrite(externalDropAndRecreate(tableRef, columnMapping = false)) {
        checkAnswer(spark.sql("SELECT * FROM v"), Seq.empty)
      }
    }
  }

  test("[1] scenario 5 external: temp view after external DROP/ADD column " +
      "same name same type (column mapping)") {
    withRefreshTable { tableRef =>
      createColumnMappingTable(tableRef)
      insertInitialData(tableRef)
      spark.table(tableRef).filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), if (strictConnect) Seq.empty else Seq(Row(1, 100)))
      withExternalWrite(externalReplaceColumn(tableRef, "salary", None)) {
        if (strictConnect) checkAnswer(spark.sql("SELECT * FROM v"), Seq.empty)
        else assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
      }
    }
  }

  test("[1] scenario 6 external: temp view after external DROP/ADD column " +
      "same name different type (column mapping)") {
    withRefreshTable { tableRef =>
      createColumnMappingTable(tableRef)
      insertInitialData(tableRef)
      spark.table(tableRef).filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), if (strictConnect) Seq.empty else Seq(Row(1, 100)))
      withExternalWrite(externalReplaceColumn(tableRef, "salary", Some("string"))) {
        if (strictConnect) {
          assertError("INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION", "View `v`") {
            spark.sql("SELECT * FROM v").collect()
          }
        } else assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
      }
    }
  }

  // Connect omits external scenario 7 (no filesystem type-widening helper); classic only.
  if (!isConnect) {
    test("[1] scenario 7 external: temp view after external type widening INT to BIGINT") {
      withRefreshTable { tableRef =>
        createTypeWideningTable(tableRef)
        insertInitialData(tableRef)
        spark.table(tableRef).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.sql("SELECT * FROM v"), Seq(Row(1, 100)))
        withExternalWrite(externalReplaceColumn(tableRef, "salary", Some("long"))) {
          assertSchemaChangeError { spark.sql("SELECT * FROM v").collect() }
        }
      }
    }
  }
}
