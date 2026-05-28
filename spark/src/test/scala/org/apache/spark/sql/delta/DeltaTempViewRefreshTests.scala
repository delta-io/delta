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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

/**
 * Section [1]: Temp views with stored plans.
 *
 * Tests that temp views backed by Delta tables correctly handle data changes,
 * schema changes (ADD/DROP COLUMN, type widening), and table recreation.
 * Covers both session-initiated and external (LogStore-bypassing) mutations.
 */
trait DeltaTempViewRefreshTests {
  self: DeltaTableRefreshTestBase with QueryTest with SharedSparkSession with DeltaSQLCommandTest =>

  import testImplicits._

  // ---------------------------------------------------------------------------
  // Section [1]: Temp views with stored plans (session writes)
  // ---------------------------------------------------------------------------

  test("[1.1] temp view picks up writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] scenario 2: temp view with ADD COLUMN preserves original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 catalog caches stale schema after ALTER TABLE ADD COLUMN
        checkError(
          exception = intercept[AnalysisException] {
            writerSql("INSERT INTO t VALUES (2, 200, -1)")
          },
          condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
          parameters = Map("tableName" -> ".*", "tableColumns" -> ".*",
            "dataColumns" -> ".*", "reason" -> ".*"),
          matchPVals = true)
      } else {
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // View preserves original schema (id, salary) but picks up new data
      checkAnswer(
        sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[1] scenario 3: temp view with DROP COLUMN throws (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 4: temp view after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      if (v2EnableMode == "STRICT") {
        // After DSv2 migration, SQL view behavior resolves tables by name.
        // Column IDs are not captured, so drop/recreate works.
        checkAnswer(sql("SELECT * FROM v"), Seq.empty)
      } else {
        // Column IDs changed, so reading the view should fail
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("SELECT * FROM v").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
          parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
          matchPVals = true)
      }
    }
  }

  test("[1] scenario 4: temp view after DROP and recreate table (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // Without column mapping, no column ID check. New table is empty.
      checkAnswer(sql("SELECT * FROM v"), Seq.empty)
    }
  }

  test("[1] scenario 5: temp view after DROP/ADD column same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      if (v2EnableMode == "STRICT") {
        // After DSv2 migration, SQL views resolve columns by name, not column ID.
        // The new salary column is empty (null) for the existing row.
        checkAnswer(sql("SELECT * FROM v"), Row(1, null))
      } else {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("SELECT * FROM v").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
          parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
          matchPVals = true)
      }
    }
  }

  test("[1] scenario 6: temp view after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 7: temp view after ALTER COLUMN TYPE INT to BIGINT") {
    withTable("t") {
      sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  // ---------------------------------------------------------------------------
  // Section [1] external: Temp views with external modifications
  // These test the "Connector w/ cache" behavior from the design doc.
  // ---------------------------------------------------------------------------

  test("[1] scenario 1 external: temp view with external data write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))

      checkAnswer(
        sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[1] scenario 2 external: temp view with external ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val newSchema = currentMetadata.schema
        .add("new_column", IntegerType, nullable = true)
      val newMetadata = currentMetadata.copy(schemaString = newSchema.json)

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalCommit(
            "t",
            Seq((2, 200, -1)).toDF("id", "salary", "new_column"),
            newMetadata = Some(newMetadata))
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalCommit(
        "t",
        Seq((2, 200, -1)).toDF("id", "salary", "new_column"),
        newMetadata = Some(newMetadata))

      // View preserves original schema (id, salary) but picks up new data
      checkAnswer(
        sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[1] scenario 3 external: temp view with external DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val newSchema = StructType(
        currentMetadata.schema.fields.filterNot(_.name == "salary"))
      val newMetadata = currentMetadata.copy(schemaString = newSchema.json)

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalMetadataOnlyCommit("t", newMetadata)
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalMetadataOnlyCommit("t", newMetadata)

      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
        parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
        matchPVals = true)
      }
    }
  }

  test("[1] scenario 4 external: temp view after external DROP and recreate " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalDropAndRecreateCommit("t", columnMapping = true)
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalDropAndRecreateCommit("t", columnMapping = true)

      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
        parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
        matchPVals = true)
      }
    }
  }

  test("[1] scenario 4 external: temp view after external DROP and recreate " +
      "(no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalDropAndRecreateCommit("t", columnMapping = false)
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalDropAndRecreateCommit("t", columnMapping = false)

      // Without column mapping, no column ID check. Existing data is removed.
      checkAnswer(sql("SELECT * FROM v"), Seq.empty)
      }
    }
  }

  test("[1] scenario 5 external: temp view after external DROP/ADD column " +
      "same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val oldMaxId = currentMetadata.columnMappingMaxId
      val newSalaryId = oldMaxId + 1
      val idField = currentMetadata.schema("id")
      val newSalaryField = buildColumnMappingField(
        name = "salary", dataType = IntegerType, nullable = true, columnId = newSalaryId)
      val newSchema = StructType(Seq(idField, newSalaryField))
      val newMetadata = currentMetadata.copy(
        schemaString = newSchema.json,
        configuration = currentMetadata.configuration +
          (DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> newSalaryId.toString))

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalMetadataOnlyCommit("t", newMetadata)
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalMetadataOnlyCommit("t", newMetadata)

      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
        parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
        matchPVals = true)
      }
    }
  }

  test("[1] scenario 6 external: temp view after external DROP/ADD column " +
      "same name different type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val oldMaxId = currentMetadata.columnMappingMaxId
      val newSalaryId = oldMaxId + 1
      val idField = currentMetadata.schema("id")
      val newSalaryField = buildColumnMappingField(
        name = "salary", dataType = StringType, nullable = true, columnId = newSalaryId)
      val newSchema = StructType(Seq(idField, newSalaryField))
      val newMetadata = currentMetadata.copy(
        schemaString = newSchema.json,
        configuration = currentMetadata.configuration +
          (DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> newSalaryId.toString))

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalMetadataOnlyCommit("t", newMetadata)
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalMetadataOnlyCommit("t", newMetadata)

      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
        parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
        matchPVals = true)
      }
    }
  }

  test("[1] scenario 7 external: temp view after external type widening INT to BIGINT") {
    withTable("t") {
      sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      // External type widening: change salary from INT to BIGINT
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val newSchema = StructType(
        currentMetadata.schema.fields.map { field =>
          if (field.name == "salary") field.copy(dataType = LongType) else field
        })
      val newMetadata = currentMetadata.copy(schemaString = newSchema.json)

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalMetadataOnlyCommit("t", newMetadata)
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalMetadataOnlyCommit("t", newMetadata)

      checkError(
        exception = intercept[DeltaAnalysisException] {
          sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
        parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
        matchPVals = true)
      }
    }
  }
}
