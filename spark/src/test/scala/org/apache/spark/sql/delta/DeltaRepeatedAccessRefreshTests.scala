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
import org.apache.spark.sql.types.IntegerType

/**
 * Section [2]: Repeated table access with external changes.
 *
 * Tests that repeated sql() calls correctly reflect both session
 * and external mutations (data writes, schema changes, drop/recreate).
 */
trait DeltaRepeatedAccessRefreshTests {
  self: DeltaTableRefreshTestBase with QueryTest with SharedSparkSession with DeltaSQLCommandTest =>

  import testImplicits._

  // ---------------------------------------------------------------------------
  // Section [2]: Repeated table access with external changes
  // ---------------------------------------------------------------------------

  test("[2] scenario 1: repeated access picks up new data") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2] scenario 2: repeated access reflects schema changes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

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

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[2] scenario 3: repeated access after drop and recreate") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      checkAnswer(sql("SELECT * FROM t"), Seq.empty)
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2] external: Repeated table access with external modifications
  // ---------------------------------------------------------------------------

  test("[2] scenario 1 external: repeated access picks up external data") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[2] scenario 2 external: repeated access reflects external schema change") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

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

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[2] scenario 3 external: repeated access after external DROP and recreate") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalDropAndRecreateCommit("t", columnMapping = false)
        }
        assert(exception.getMessage != null)
      } else {
      writeExternalDropAndRecreateCommit("t", columnMapping = false)

      checkAnswer(sql("SELECT * FROM t"), Seq.empty)
      }
    }
  }
}
