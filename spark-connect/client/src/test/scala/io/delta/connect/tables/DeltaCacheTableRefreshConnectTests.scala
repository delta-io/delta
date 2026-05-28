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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.DeltaQueryTest

trait DeltaCacheTableRefreshConnectTests {
  self: DeltaTableRefreshConnectTestBase with DeltaQueryTest with RemoteSparkSession =>

  // ---------------------------------------------------------------------------
  // Section [5]: CACHE TABLE impact on reads (Connect)
  //
  // Note on external writes: In Connect, all writes go through the same
  // server-side DeltaLog (shared singleton cache). We cannot simulate true
  // external writes (bypassing DeltaLog) from the Connect client. These tests
  // document the same-JVM behavior where all writes update DeltaLog.currentSnapshot,
  // causing Delta's PrepareDeltaScan to discover changes and break the cache.
  //
  // For true external write behavior (cache pinning), see the classic suite's
  // scenarios 6b-6e which use writeExternalCommit via LogStore.
  // ---------------------------------------------------------------------------

  test("[5] connect scenario 1: CACHE TABLE with same-JVM writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Same-JVM write updates DeltaLog.currentSnapshot, so Delta's
      // PrepareDeltaScan discovers the change and breaks the cache.
      // Doc says: (1,100) only for true external writes, but same-JVM
      // writes bypass the cache pinning mechanism.
      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 2: session write then same-JVM external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Session write invalidates cache (via SPARK-55631 refreshCache)
      spark.sql("INSERT INTO t VALUES (2, 200)")

      // Same-JVM "external" write also updates DeltaLog.currentSnapshot
      writerSql("INSERT INTO t VALUES (3, 300)")

      // Both writes visible because both go through same-JVM DeltaLog.
      // Doc says: (1,100),(2,200) only for true external writes.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 3: schema change breaks cache") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Schema change via same-JVM SQL goes through AlterTableExec which
      // triggers refreshCache (SPARK-55631), invalidating the cache.
      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // Doc says: schema changes break table state pinning.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 4: session schema change with external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Session schema change invalidates cache (SPARK-55631)
      spark.sql("ALTER TABLE t ADD COLUMN new_column INT")

      // Same-JVM "external" write
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // Both visible because session ALTER TABLE broke the cache and
      // same-JVM write updated DeltaLog.currentSnapshot.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 5: drop and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // Doc says: empty table after drop and recreate.
      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)

      spark.sql("UNCACHE TABLE IF EXISTS t")
    }
  }

  // ---------------------------------------------------------------------------
  // Section [5] continued: True external write simulation via filesystem
  // Uses writeExternalCommitViaFilesystem to write commit files directly to
  // disk, bypassing the server's DeltaLog. This is the Connect equivalent
  // of the classic suite's writeExternalCommit / scenarios 6b-6e.
  // ---------------------------------------------------------------------------

  test("[5] connect scenario 6b: CACHE TABLE pins data against external writes") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6b AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6b"), Row(1, 100))

      // True external write via filesystem -- server's DeltaLog not updated
      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      // Doc says: (1,100) only -- CACHE TABLE pins data against external writes.
      // The server's DeltaTableV2.lazy val snapshot is pinned and CacheManager
      // matches the cached plan.
      checkAnswer(spark.sql("SELECT * FROM cached_6b"), Row(1, 100))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6b")
    }
  }

  test("[5] connect scenario 6c: session write invalidates, external not visible") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6c AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6c"), Row(1, 100))

      // Session write invalidates cache
      spark.sql(s"INSERT INTO delta.`$path` VALUES (2, 200)")

      // True external write via filesystem
      writeExternalCommitViaFilesystem(path, Seq((3, 300)))

      // Doc says: (1,100),(2,200) -- session write visible, external not.
      // After session write invalidates the cache, the next query re-analyzes.
      // The server's DeltaLog was updated by the session write but not the
      // external filesystem write.
      checkAnswer(
        spark.sql("SELECT * FROM cached_6c ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6c")

      // After uncaching, fresh query discovers all data including external write.
      // The session INSERT updated server's DeltaLog, and UNCACHE triggers
      // a deltaLog.update() that discovers the external commit.
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  // Scenarios 6d (external schema change with CACHE) and 6e (session schema
  // change then external write) are omitted from the Connect suite. In Connect,
  // external filesystem writes bypass the server's DeltaLog entirely, and the
  // DeltaLog's async update mechanism may or may not discover the external
  // commit depending on timing. This makes the behavior non-deterministic:
  // sometimes the cached view returns old data (1 row), sometimes the DeltaLog
  // discovers the external commit and returns fresh data (2 rows). The classic
  // suite tests these scenarios deterministically because writeExternalCommit
  // uses LogStore.write() which the DeltaLog's listing reliably discovers.
}
