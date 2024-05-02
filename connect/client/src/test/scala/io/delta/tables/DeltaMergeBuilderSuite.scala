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
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.test.{QueryTest, SQLHelper}

class DeltaMergeBuilderSuite extends QueryTest with SQLHelper {
  private def writeTargetTable(path: String): Unit = {
    val session = spark
    import session.implicits._
    Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4)).toDF("key", "value")
      .write.mode("overwrite").format("delta").save(path)
  }

  private def testSource = {
    val session = spark
    import session.implicits._
    Seq(("a", -1), ("b", 0), ("e", -5), ("f", -6)).toDF("k", "v")
  }

  test("string expressions in merge conditions and assignments") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable
        .merge(testSource, "key = k")
        .whenMatched().updateExpr(Map("value" -> "value + v"))
        .whenNotMatched().insertExpr(Map("key" -> "k", "value" -> "v"))
        .whenNotMatchedBySource().updateExpr(Map("value" -> "value - 1"))
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 0), Row("b", 2), Row("c", 2), Row("d", 3), Row("e", -5), Row("f", -6)))
    }
  }

  test("column expressions in merge conditions and assignments") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable
        .merge(testSource, col("key") === col("k"))
        .whenMatched().update(Map("value" -> (col("value") + col("v"))))
        .whenNotMatched().insert(Map("key" -> col("k"), "value" -> col("v")))
        .whenNotMatchedBySource().update(Map("value" -> (col("value") - 1)))
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 0), Row("b", 2), Row("c", 2), Row("d", 3), Row("e", -5), Row("f", -6)))
    }
  }

  test("multiple when matched then update clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable
        .merge(testSource, expr("key = k"))
        .whenMatched("key = 'a'").updateExpr(Map("value" -> "5"))
        .whenMatched().updateExpr(Map("value" -> "0"))
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 5), Row("b", 0), Row("c", 3), Row("d", 4)))
    }
  }

  test("multiple when matched then delete clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable
        .merge(testSource, "key = k")
        .whenMatched("key = 'a'").delete()
        .whenMatched().delete()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("c", 3), Row("d", 4)))
    }
  }

  test("redundant when matched then update and delete clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable
        .merge(testSource, col("key") === col("k"))
        .whenMatched("key = 'a'").updateExpr(Map("value" -> "5"))
        .whenMatched("key = 'a'").updateExpr(Map("value" -> "0"))
        .whenMatched("key = 'b'").updateExpr(Map("value" -> "6"))
        .whenMatched("key = 'b'").delete()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 5), Row("b", 6), Row("c", 3), Row("d", 4)))
    }
  }

  test("interleaved when matched then update and delete clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.as("t")
        .merge(testSource, col("t.key") === col("k"))
        .whenMatched("t.key = 'a'").delete()
        .whenMatched("t.key = 'a'").updateExpr(Map("value" -> "5"))
        .whenMatched("t.key = 'b'").delete()
        .whenMatched().updateExpr(Map("value" -> "6"))
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("c", 3), Row("d", 4)))
    }
  }

  test("multiple when not matched then insert clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.as("t")
        .merge(testSource.toDF("key", "value").as("s"), col("t.key") === col("s.key"))
        .whenNotMatched("s.key = 'e'").insertExpr(Map("t.key" -> "s.key", "t.value" -> "5"))
        .whenNotMatched().insertAll()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 1), Row("b", 2), Row("c", 3), Row("d", 4), Row("e", 5), Row("f", -6)))
    }
  }

  test("redundant when not matched then insert clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable
        .merge(testSource, expr("key = k"))
        .whenNotMatched("k = 'e'").insertExpr(Map("key" -> "k", "value" -> "5"))
        .whenNotMatched("k = 'e'").insertExpr(Map("key" -> "k", "value" -> "6"))
        .whenNotMatched("k = 'f'").insertExpr(Map("key" -> "k", "value" -> "7"))
        .whenNotMatched("k = 'f'").insertExpr(Map("key" -> "k", "value" -> "8"))
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 1), Row("b", 2), Row("c", 3), Row("d", 4), Row("e", 5), Row("f", 7)))
    }
  }

  test("multiple when not matched by source then update clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.merge(testSource, expr("key = k"))
        .whenNotMatchedBySource("key = 'c'").updateExpr(Map("value" -> "5"))
        .whenNotMatchedBySource().updateExpr(Map("value" -> "0"))
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 1), Row("b", 2), Row("c", 5), Row("d", 0)))
    }
  }

  test("multiple when not matched by source then delete clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.merge(testSource, expr("key = k"))
        .whenNotMatchedBySource("key = 'c'").delete()
        .whenNotMatchedBySource().delete()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 1), Row("b", 2)))
    }
  }

  test("redundant when not matched by source then update and delete clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.merge(testSource, expr("key = k"))
        .whenNotMatchedBySource("key = 'c'").updateExpr(Map("value" -> "5"))
        .whenNotMatchedBySource("key = 'c'").updateExpr(Map("value" -> "0"))
        .whenNotMatchedBySource("key = 'd'").updateExpr(Map("value" -> "6"))
        .whenNotMatchedBySource("key = 'd'").delete()
        .whenNotMatchedBySource().delete()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 1), Row("b", 2), Row("c", 5), Row("d", 6)))
    }
  }


  test("interleaved when not matched by source then update and delete clauses") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.merge(testSource, expr("key = k"))
        .whenNotMatchedBySource("key = 'c'").delete()
        .whenNotMatchedBySource("key = 'c'").updateExpr(Map("value" -> "5"))
        .whenNotMatchedBySource("key = 'd'").delete()
        .whenNotMatchedBySource().updateExpr(Map("value" -> "6"))
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", 1), Row("b", 2)))
    }
  }

  test("string expressions in all conditions and assignments") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable
        .merge(testSource, "key = k")
        .whenMatched("k = 'a'").updateExpr(Map("value" -> "v + 0"))
        .whenMatched("k = 'b'").delete()
        .whenNotMatched("k = 'e'").insertExpr(Map("key" -> "k", "value" -> "v + 0"))
        .whenNotMatchedBySource("key = 'c'").updateExpr(Map("value" -> "value + 0"))
        .whenNotMatchedBySource("key = 'd'").delete()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", -1), Row("c", 3), Row("e", -5)))
    }
  }

  test("column expressions in all conditions and assignments") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable
        .merge(testSource, expr("key = k"))
        .whenMatched(expr("k = 'a'")).update(Map("value" -> (col("v") + 0)))
        .whenMatched(expr("k = 'b'")).delete()
        .whenNotMatched(expr("k = 'e'")).insert(Map("key" -> col("k"), "value" -> (col("v") + 0)))
        .whenNotMatchedBySource(expr("key = 'c'")).update(Map("value" -> (col("value") + 0)))
        .whenNotMatchedBySource(expr("key = 'd'")).delete()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", -1), Row("c", 3), Row("e", -5)))
    }
  }

  test("no clause conditions and insertAll/updateAll + aliases") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.as("t")
        .merge(testSource.toDF("key", "value").as("s"), expr("t.key = s.key"))
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", -1), Row("b", 0), Row("c", 3), Row("d", 4), Row("e", -5), Row("f", -6)))
    }
  }

  test("string expressions in all clause conditions and insertAll/updateAll + aliases") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.as("t")
        .merge(testSource.toDF("key", "value").as("s"), "t.key = s.key")
        .whenMatched("s.key = 'a'").updateAll()
        .whenNotMatched("s.key = 'e'").insertAll()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", -1), Row("b", 2), Row("c", 3), Row("d", 4), Row("e", -5)))
    }
  }

  test("column expressions in all clause conditions and insertAll/updateAll + aliases") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      writeTargetTable(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.as("t")
        .merge(testSource.toDF("key", "value").as("s"), expr("t.key = s.key"))
        .whenMatched(expr("s.key = 'a'")).updateAll()
        .whenNotMatched(expr("s.key = 'e'")).insertAll()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(Row("a", -1), Row("b", 2), Row("c", 3), Row("d", 4), Row("e", -5)))
    }
  }

  test("automatic schema evolution") {
    val session = spark
    import session.implicits._

    withTempPath { dir =>
      val path = dir.getAbsolutePath
      Seq("a", "b", "c", "d").toDF("key")
        .write.mode("overwrite").format("delta").save(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      withSQLConf("spark.databricks.delta.schema.autoMerge.enabled" -> "true") {
        deltaTable.as("t")
          .merge(testSource.toDF("key", "value").as("s"), expr("t.key = s.key"))
          .whenMatched().updateAll()
          .whenNotMatched().insertAll()
          .execute()
      }

      checkAnswer(
        deltaTable.toDF,
        Seq(
          Row("a", -1), Row("b", 0), Row("c", null), Row("d", null), Row("e", -5), Row("f", -6)))
    }
  }

  test("merge dataframe with many columns") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      var df1 = spark.range(1).toDF
      val numColumns = 100
      for (i <- 0 until numColumns) {
        df1 = df1.withColumn(s"col$i", col("id"))
      }
      df1.write.mode("overwrite").format("delta").save(path)
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, path)

      var df2 = spark.range(1).toDF
      for (i <- 0 until numColumns) {
        df2 = df2.withColumn(s"col$i", col("id") + 1)
      }

      deltaTable
        .as("t")
        .merge(df2.as("s"), "s.id = t.id")
        .whenMatched().updateAll()
        .execute()

      checkAnswer(
        deltaTable.toDF,
        Seq(df2.collectAsList().get(0)))
    }
  }
}
