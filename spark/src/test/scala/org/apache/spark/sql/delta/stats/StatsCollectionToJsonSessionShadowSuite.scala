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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.delta.{DeltaLog, DeltaTableProvider}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.delta.util.JsonUtils.toJsonColumn

import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.api.java.{UDF1, UDF2}
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

/** Session UDFs named to_json must not replace Delta's internal JSON serialization. */
class StatsCollectionToJsonSessionShadowSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaTableProvider {

  private val shadowed = ToJsonShadow.result

  for (arity <- Seq(1, 2)) {
    test(s"write stats ignore a $arity-arg session to_json UDF") {
      withToJsonUdf(arity) { session =>
        withTempDir { dir =>
          val path = dir.getCanonicalPath
          writeTestTable(session, path)
          assertValidFileStats(session, path)
        }
      }
    }

    test(s"recomputed stats ignore a $arity-arg session to_json UDF") {
      withToJsonUdf(arity) { session =>
        withTempDir { dir =>
          val path = dir.getCanonicalPath
          session.conf.set(DeltaSQLConf.DELTA_COLLECT_STATS.key, "false")
          writeTestTable(session, path)
          val deltaLog = DeltaLog.forTable(session, path)
          assert(deltaLog.update().allFiles.collect().forall(_.stats == null))
          session.conf.set(DeltaSQLConf.DELTA_COLLECT_STATS.key, "true")
          StatisticsCollection.recompute(session, deltaLog, catalogTable = None)
          assertValidFileStats(session, path)
        }
      }
    }

    test(s"JSON columns ignore a $arity-arg session to_json UDF") {
      withToJsonUdf(arity) { session =>
        val options = if (arity == 1) Map.empty[String, String] else Map("pretty" -> "false")
        checkAnswer(
          session.range(1).select(toJsonColumn(struct(col("id")), options)),
          Row("""{"id":0}"""))
      }
    }

    test(s"public to_json follows normal resolution for a $arity-arg session UDF") {
      withToJsonUdf(arity) { session =>
        val options = if (arity == 1) Map.empty[String, String] else Map("pretty" -> "false")
        // Unlike toJsonColumn, the public to_json goes through function-name resolution, so a
        // same-named session UDF may or may not shadow the builtin depending on the engine's
        // function-resolution order. Accept either outcome rather than pinning one precedence.
        val result = session.range(1)
          .select(to_json(struct(col("id")), options))
          .head()
          .getString(0)
        assert(
          Set(shadowed, """{"id":0}""").contains(result),
          s"unexpected public to_json result: $result")
      }
    }
  }

  test("JSON columns preserve nulls and options") {
    withSQLConf(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "true") {
      val data = sql("""
        SELECT named_struct('id', 1, 'missing', CAST(NULL AS STRING)) AS value
        UNION ALL
        SELECT CAST(NULL AS STRUCT<id: INT, missing: STRING>) AS value
      """)
      checkAnswer(
        data.select(toJsonColumn(col("value"))),
        Seq(Row("""{"id":1}"""), Row(null)))
      checkAnswer(
        data.select(toJsonColumn(col("value"), Map("ignoreNullFields" -> "false"))),
        Seq(Row("""{"id":1,"missing":null}"""), Row(null)))
    }
  }

  test("JSON columns resolve the session timezone and honor overrides") {
    val options = Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss")
    val json = toJsonColumn(col("value"), options)
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val data = sql("SELECT named_struct('ts', timestamp_micros(0)) AS value")
      checkAnswer(data.select(json), Row("""{"ts":"1969-12-31 16:00:00"}"""))
      checkAnswer(
        data.select(toJsonColumn(col("value"), options + ("timeZone" -> "UTC"))),
        Row("""{"ts":"1970-01-01 00:00:00"}"""))
    }
  }


  private def writeTestTable(session: SparkSession, path: String): Unit = {
    session.range(3).coalesce(1).toDF("id").write.format(writeFormat).save(path)
  }

  private def withToJsonUdf(arity: Int)(f: SparkSession => Unit): Unit = {
    // A fresh session prevents replacing or dropping the suite's builtin registry entry.
    val session = spark.newSession()
    arity match {
      case 1 =>
        session.udf.register("to_json", ToJsonShadow.oneArg, StringType)
      case 2 =>
        session.udf.register("to_json", ToJsonShadow.twoArg, StringType)
      case other =>
        fail(s"unsupported to_json UDF arity: $other")
    }
    session.withActive {
      f(session)
    }
  }

  private def assertValidFileStats(session: SparkSession, path: String): Unit = {
    val stats = DeltaLog.forTable(session, path).update().allFiles.collect().map(_.stats)
    assert(stats.nonEmpty, "expected at least one AddFile")
    stats.foreach { raw =>
      assert(raw != null && raw.nonEmpty, "expected non-empty stats JSON")
      assert(!raw.contains(shadowed), s"session to_json UDF replaced file stats: $raw")
      val tree = JsonUtils.mapper.readTree(raw)
      assert(tree.path("numRecords").asLong() == 3L, s"unexpected numRecords: $raw")
      assert(tree.path("minValues").has("id"), s"minValues missing id: $raw")
      assert(tree.path("minValues").path("id").asLong() == 0L, s"unexpected minValues: $raw")
      assert(tree.path("maxValues").path("id").asLong() == 2L, s"unexpected maxValues: $raw")
      assert(tree.path("nullCount").has("id"), s"nullCount missing id: $raw")
      assert(tree.path("nullCount").path("id").asLong() == 0L, s"unexpected nullCount: $raw")
    }
  }
}

private object ToJsonShadow {
  val result: String = "SHADOWED_JSON"

  val oneArg: UDF1[Object, String] = new UDF1[Object, String] with Serializable {
    override def call(t1: Object): String = result
  }

  val twoArg: UDF2[Object, Object, String] = new UDF2[Object, Object, String] with Serializable {
    override def call(t1: Object, t2: Object): String = result
  }
}
