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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * Section [5]: CACHE TABLE impact on reads.
 *
 * Tests how CACHE TABLE behaves when cached tables are mutated by
 * session SQL or external (LogStore-bypassing) writes. Demonstrates
 * cache pinning, invalidation, and the two-layer cache interaction
 * between Spark's CacheManager and Delta's DeltaLog.
 */
trait DeltaCacheTableRefreshTests {
  self: DeltaTableRefreshTestBase with QueryTest with SharedSparkSession with DeltaSQLCommandTest =>

  import testImplicits._

  // ---------------------------------------------------------------------------
  // Section [5]: CACHE TABLE impact on reads
  // ---------------------------------------------------------------------------

  test("[5] scenario 1: CACHE TABLE with external writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))
        }
        assert(exception.getMessage != null)
        sql("UNCACHE TABLE t")
      } else {
      // True external write bypassing the session catalog and DeltaLog
      writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))

      // CACHE TABLE pins data against true external writes (SPARK-54022).
      // writeExternalCommit bypasses DeltaLog, so the cached plan matches
      // regardless of V2 mode. This matches the existing OSS Delta behavior
      // documented in the design doc.
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      sql("UNCACHE TABLE t")
      }
    }
  }

  test("[5] scenario 2: session write invalidates cache then external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session write invalidates the cache
      sql("INSERT INTO t VALUES (2, 200)")

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalCommit("t", Seq((3, 300)).toDF("id", "salary"))
        }
        assert(exception.getMessage != null)
        sql("UNCACHE TABLE t")
      } else {
      // True external write bypassing the session catalog and DeltaLog
      writeExternalCommit("t", Seq((3, 300)).toDF("id", "salary"))

      // Session write invalidates cache, but the subsequent external write
      // via writeExternalCommit bypasses DeltaLog and is invisible.
      // This matches the existing OSS Delta behavior documented in the
      // design doc (SPARK-54022).
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      sql("UNCACHE TABLE t")
      }
    }
  }

  test("[5] scenario 3: external schema change") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // External schema change + data write via direct filesystem commit
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
        sql("UNCACHE TABLE t")
      } else {
      writeExternalCommit(
        "t",
        Seq((2, 200, -1)).toDF("id", "salary", "new_column"),
        newMetadata = Some(newMetadata))

      // In NONE/AUTO mode, schema change breaks the plan-shape match in
      // CacheManager, so the cache is effectively invalidated.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      sql("UNCACHE TABLE t")
      }
    }
  }

  test("[5] scenario 4: session schema change with external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session schema change
      sql("ALTER TABLE t ADD COLUMN new_column INT")

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalCommit("t", Seq((2, 200, -1)).toDF("id", "salary", "new_column"))
        }
        assert(exception.getMessage != null)
        sql("UNCACHE TABLE t")
      } else {
      // True external write bypassing the session catalog
      writeExternalCommit("t", Seq((2, 200, -1)).toDF("id", "salary", "new_column"))

      // In NONE/AUTO mode, schema change from the session invalidates the
      // cache. Delta picks up all data including the external write.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      sql("UNCACHE TABLE t")
      }
    }
  }

  test("[5] scenario 5: external drop and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // After drop and recreate, the table is empty
      checkAnswer(sql("SELECT * FROM t"), Seq.empty)

      sql("UNCACHE TABLE IF EXISTS t")
    }
  }


  test("[5] scenario 6b: CACHE TABLE pins data against external writes") {
    // Doc scenario 1 with true external write simulation.
    // Uses writeExternalCommit (Delta equivalent of SPARK-54022's
    // catalog.loadTable().truncateTable() pattern).
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))
        }
        assert(exception.getMessage != null)
        sql("UNCACHE TABLE IF EXISTS t")
      } else {
      // External write bypassing the session catalog
      writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))

      // Doc says: (1,100) only -- cache pins data against external writes
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      sql("UNCACHE TABLE IF EXISTS t")

      // After uncaching, fresh query discovers the external commit.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[5] scenario 6c: session write invalidates cache, external write not visible") {
    // Doc scenario 2: session INSERT invalidates cache, then external write.
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session write invalidates the cache (via SPARK-55631 refreshCache)
      sql("INSERT INTO t VALUES (2, 200)")

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalCommit("t", Seq((3, 300)).toDF("id", "salary"))
        }
        assert(exception.getMessage != null)
        sql("UNCACHE TABLE IF EXISTS t")
      } else {
      // External write bypassing the session catalog
      writeExternalCommit("t", Seq((3, 300)).toDF("id", "salary"))

      // Doc says: (1,100),(2,200) -- session write visible, external not
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      sql("UNCACHE TABLE IF EXISTS t")

      // After uncaching, fresh query discovers all data including external write.
      // The session INSERT updated DeltaLog.currentSnapshot to version 2, and
      // UNCACHE TABLE's table resolution triggers a deltaLog.update() that
      // discovers the external commit at version 3.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
      }
    }
  }

  test("[5] scenario 6d: external schema change with CACHE") {
    // Doc scenario 3: external writer adds a column and data.
    // Writes both a Metadata action (schema change) and AddFile via
    // writeExternalCommit, bypassing AlterTableExec.refreshCache (SPARK-55631).
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Build new schema with added column
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val newSchema = currentMetadata.schema
        .add("new_column", org.apache.spark.sql.types.IntegerType, nullable = true)
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
        sql("UNCACHE TABLE IF EXISTS t")
      } else {
      // External schema change + data write
      writeExternalCommit(
        "t",
        Seq((2, 200, -1)).toDF("id", "salary", "new_column"),
        newMetadata = Some(newMetadata))

      // deltaLog.update() lists filesystem, discovers the schema change.
      // New schema in analyzed plan doesn't match cached plan -> cache miss
      // -> fresh data visible. Matches doc: schema changes break table state pinning.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      sql("UNCACHE TABLE IF EXISTS t")

      // After uncaching, fresh query discovers the external commit.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[5] scenario 6e: session schema change then external write") {
    // Doc scenario 4: session ALTER TABLE invalidates cache (SPARK-55631),
    // then external data write.
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session schema change invalidates cache via AlterTableExec.refreshCache
      sql("ALTER TABLE t ADD COLUMN new_column INT")

      if (v2EnableMode == "STRICT") {
        // STRICT mode: V2 commit path leaves DeltaLog snapshot stale, version conflict
        val exception = intercept[java.nio.file.FileAlreadyExistsException] {
          writeExternalCommit("t", Seq((2, 200, -1)).toDF("id", "salary", "new_column"))
        }
        assert(exception.getMessage != null)
        sql("UNCACHE TABLE IF EXISTS t")
      } else {
      // External data write
      writeExternalCommit("t", Seq((2, 200, -1)).toDF("id", "salary", "new_column"))

      // Session schema change breaks cache. Next query re-analyzes.
      // Listing discovers external write too. Matches doc: (1,100,null),(2,200,-1)
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      sql("UNCACHE TABLE IF EXISTS t")

      // After uncaching, fresh query discovers all data including external write.
      // The session ALTER TABLE updated DeltaLog.currentSnapshot, and
      // UNCACHE TABLE's table resolution triggers a deltaLog.update() that
      // discovers the external commit.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }
}
