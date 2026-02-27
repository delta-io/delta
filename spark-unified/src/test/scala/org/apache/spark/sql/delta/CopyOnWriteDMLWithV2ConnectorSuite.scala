/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, V2ForceTest}

/**
 * Tests for Copy-on-Write DML operations (DELETE, UPDATE, MERGE) using the
 * Kernel-based V2 connector (SparkTable with SupportsRowLevelOperations).
 */
class CopyOnWriteDMLWithV2ConnectorSuite
  extends QueryTest
  with DeltaSQLCommandTest
  with V2ForceTest {

  private def withTestTable(f: String => Unit): Unit = {
    val tableName = "cow_test_table"
    try {
      f(tableName)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  private def createAndPopulateTable(tableName: String): Unit = {
    spark.sql(
      s"""CREATE TABLE $tableName (id BIGINT, data STRING)
         |USING delta""".stripMargin)
    spark.sql(
      s"""INSERT INTO $tableName VALUES
         |(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')""".stripMargin)
  }

  // ---------- DELETE tests ----------

  test("DELETE: delete single row") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(s"DELETE FROM $table WHERE id = 3")
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "b"), Row(4, "d"), Row(5, "e"))
      )
    }
  }

  test("DELETE: delete multiple rows") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(s"DELETE FROM $table WHERE id >= 4")
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
      )
    }
  }

  test("DELETE: delete all rows") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(s"DELETE FROM $table WHERE true")
      checkAnswer(spark.table(table), Seq.empty)
    }
  }

  test("DELETE: delete no rows (condition matches nothing)") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(s"DELETE FROM $table WHERE id = 999")
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d"), Row(5, "e"))
      )
    }
  }

  // ---------- UPDATE tests ----------

  test("UPDATE: update single row") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(s"UPDATE $table SET data = 'updated' WHERE id = 2")
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "updated"), Row(3, "c"), Row(4, "d"), Row(5, "e"))
      )
    }
  }

  test("UPDATE: update multiple rows") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(s"UPDATE $table SET data = 'big' WHERE id > 3")
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "big"), Row(5, "big"))
      )
    }
  }

  test("UPDATE: update all rows") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(s"UPDATE $table SET data = 'all'")
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(1, "all"), Row(2, "all"), Row(3, "all"), Row(4, "all"), Row(5, "all"))
      )
    }
  }

  // ---------- MERGE tests ----------

  test("MERGE: matched update") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(
        s"""MERGE INTO $table t
           |USING (SELECT 2 AS id, 'merged' AS data) s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.data = s.data""".stripMargin)
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "merged"), Row(3, "c"), Row(4, "d"), Row(5, "e"))
      )
    }
  }

  test("MERGE: matched delete") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(
        s"""MERGE INTO $table t
           |USING (SELECT 3 AS id) s
           |ON t.id = s.id
           |WHEN MATCHED THEN DELETE""".stripMargin)
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "b"), Row(4, "d"), Row(5, "e"))
      )
    }
  }

  test("MERGE: not matched insert") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(
        s"""MERGE INTO $table t
           |USING (SELECT 6 AS id, 'f' AS data) s
           |ON t.id = s.id
           |WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)""".stripMargin)
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d"), Row(5, "e"), Row(6, "f"))
      )
    }
  }

  test("MERGE: matched update + not matched insert") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(
        s"""MERGE INTO $table t
           |USING (
           |  SELECT * FROM VALUES (2, 'updated'), (6, 'new') AS s(id, data)
           |) s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.data = s.data
           |WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)""".stripMargin)
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(
          Row(1, "a"), Row(2, "updated"), Row(3, "c"),
          Row(4, "d"), Row(5, "e"), Row(6, "new"))
      )
    }
  }

  // ---------- Sequential DML tests ----------

  test("sequential DELETE then UPDATE") {
    withTestTable { table =>
      createAndPopulateTable(table)
      spark.sql(s"DELETE FROM $table WHERE id = 1")
      spark.sql(s"UPDATE $table SET data = 'x' WHERE id = 2")
      checkAnswer(
        spark.table(table).orderBy("id"),
        Seq(Row(2, "x"), Row(3, "c"), Row(4, "d"), Row(5, "e"))
      )
    }
  }
}
