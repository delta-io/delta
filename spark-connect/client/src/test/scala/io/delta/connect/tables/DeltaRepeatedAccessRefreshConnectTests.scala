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

trait DeltaRepeatedAccessRefreshConnectTests {
  self: DeltaTableRefreshConnectTestBase with DeltaQueryTest with RemoteSparkSession =>

  // ---------------------------------------------------------------------------
  // Section [2]: Repeated table access with external changes (Connect)
  // ---------------------------------------------------------------------------

  test("[2] connect scenario 1: repeated access picks up new data") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2] connect scenario 2: repeated access reflects schema changes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[2] connect scenario 3: repeated access after drop and recreate") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2] external: Repeated table access with external modifications
  // ---------------------------------------------------------------------------

  test("[2] connect scenario 1 external: repeated access picks up external data") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Row(1, 100))

      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2] connect scenario 2 external: repeated access reflects external schema change") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Row(1, 100))

      writeExternalSchemaChangeCommitViaFilesystem(path, Seq((2, 200, -1)))

      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[2] connect scenario 3 external: repeated access after external DROP and recreate") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Row(1, 100))

      writeExternalDropAndRecreateViaFilesystem(path)

      checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Seq.empty)
    }
  }
}
