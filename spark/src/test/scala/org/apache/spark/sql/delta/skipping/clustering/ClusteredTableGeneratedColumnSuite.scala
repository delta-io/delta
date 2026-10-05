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

package org.apache.spark.sql.delta.skipping.clustering

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for liquid clustering on tables that contain generated columns.
 *
 * Covers:
 *  - Creating a clustered table where the clustering key is a generated column.
 *  - Inserting data and verifying the generated column is computed automatically.
 *  - Running OPTIMIZE on such a table.
 *  - Using ALTER TABLE CLUSTER BY to switch clustering to/from a generated column.
 *  - Verifying that data skipping works through a generated clustering column.
 *
 * Relates to: https://github.com/delta-io/delta/issues/3248
 */
class ClusteredTableGeneratedColumnSuite
    extends QueryTest
    with SharedSparkSession
    with ClusteredTableTestUtils
    with DeltaSQLCommandTest {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_UPDATE_CATALOG_ENABLED.key, "true")
  }

  test("create clustered table with generated column as cluster key") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  id BIGINT,
          |  ts TIMESTAMP,
          |  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
          |) USING DELTA
          |CLUSTER BY (event_date)""".stripMargin)

      verifyClusteringColumns(TableIdentifier("t"), Seq("event_date"))
    }
  }

  test("create clustered table with multiple columns, one of which is generated") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  id BIGINT,
          |  ts TIMESTAMP,
          |  category STRING,
          |  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
          |) USING DELTA
          |CLUSTER BY (event_date, category)""".stripMargin)

      verifyClusteringColumns(TableIdentifier("t"), Seq("event_date", "category"))
    }
  }

  test("create clustered table with numeric generated column as cluster key") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  price DOUBLE,
          |  quantity INT,
          |  revenue DOUBLE GENERATED ALWAYS AS (price * quantity)
          |) USING DELTA
          |CLUSTER BY (revenue)""".stripMargin)

      verifyClusteringColumns(TableIdentifier("t"), Seq("revenue"))
    }
  }

  test("insert into clustered table populates generated column automatically") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  id BIGINT,
          |  ts TIMESTAMP,
          |  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
          |) USING DELTA
          |CLUSTER BY (event_date)""".stripMargin)

      sql(
        """INSERT INTO t (id, ts)
          |VALUES (1, TIMESTAMP'2024-03-15 10:00:00'),
          |       (2, TIMESTAMP'2024-03-16 11:00:00'),
          |       (3, TIMESTAMP'2024-03-15 22:59:00')""".stripMargin)

      checkAnswer(
        sql("SELECT id, event_date FROM t ORDER BY id"),
        Seq(
          Row(1L, java.sql.Date.valueOf("2024-03-15")),
          Row(2L, java.sql.Date.valueOf("2024-03-16")),
          Row(3L, java.sql.Date.valueOf("2024-03-15"))
        )
      )
      verifyClusteringColumns(TableIdentifier("t"), Seq("event_date"))
    }
  }

  test("optimize clustered table with generated column as cluster key") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  id BIGINT,
          |  ts TIMESTAMP,
          |  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
          |) USING DELTA
          |CLUSTER BY (event_date)""".stripMargin)

      sql("INSERT INTO t (id, ts) VALUES (1, TIMESTAMP'2024-01-01 00:00:00')")
      sql("INSERT INTO t (id, ts) VALUES (2, TIMESTAMP'2024-01-02 00:00:00')")
      sql("INSERT INTO t (id, ts) VALUES (3, TIMESTAMP'2024-01-01 12:00:00')")

      sql("OPTIMIZE t")

      verifyClusteringColumns(TableIdentifier("t"), Seq("event_date"))
    }
  }

  test("alter table cluster by to switch clustering to a generated column") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  id BIGINT,
          |  ts TIMESTAMP,
          |  category STRING,
          |  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
          |) USING DELTA
          |CLUSTER BY (category)""".stripMargin)

      verifyClusteringColumns(TableIdentifier("t"), Seq("category"))
      sql("ALTER TABLE t CLUSTER BY (event_date)")
      verifyClusteringColumns(TableIdentifier("t"), Seq("event_date"))
    }
  }

  test("alter table cluster by to switch clustering away from a generated column") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  id BIGINT,
          |  ts TIMESTAMP,
          |  category STRING,
          |  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
          |) USING DELTA
          |CLUSTER BY (event_date)""".stripMargin)

      verifyClusteringColumns(TableIdentifier("t"), Seq("event_date"))
      sql("ALTER TABLE t CLUSTER BY (category)")
      verifyClusteringColumns(TableIdentifier("t"), Seq("category"))
    }
  }

  test("query with filter on generated clustering column reads correct rows") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  id BIGINT,
          |  ts TIMESTAMP,
          |  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
          |) USING DELTA
          |CLUSTER BY (event_date)""".stripMargin)

      sql(
        """INSERT INTO t (id, ts)
          |VALUES (1, TIMESTAMP'2024-01-01 00:00:00'),
          |       (2, TIMESTAMP'2024-01-02 00:00:00'),
          |       (3, TIMESTAMP'2024-01-01 06:00:00'),
          |       (4, TIMESTAMP'2024-01-03 00:00:00')""".stripMargin)
      sql("OPTIMIZE t")

      checkAnswer(
        sql("SELECT id FROM t WHERE event_date = DATE'2024-01-01' ORDER BY id"),
        Seq(Row(1L), Row(3L))
      )
      checkAnswer(
        sql("SELECT id FROM t WHERE event_date = DATE'2024-01-02'"),
        Seq(Row(2L))
      )
    }
  }

  test("describe clustered table shows generated column and clustering info") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  id BIGINT,
          |  ts TIMESTAMP,
          |  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
          |) USING DELTA
          |CLUSTER BY (event_date)""".stripMargin)

      val describeResult = sql("DESCRIBE EXTENDED t").collect()
      val colNames = describeResult.map(_.getString(0))

      assert(colNames.contains("event_date"),
        "DESCRIBE output should include the generated column 'event_date'")
      val clusterByRow = describeResult.find(_.getString(0).toLowerCase.contains("cluster"))
      assert(clusterByRow.isDefined,
        "DESCRIBE EXTENDED output should include clustering information")
    }
  }

  test("clustering domain metadata is correct after insert and optimize") {
    withTable("t") {
      sql(
        """CREATE TABLE t (
          |  id BIGINT,
          |  ts TIMESTAMP,
          |  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
          |) USING DELTA
          |CLUSTER BY (event_date)""".stripMargin)

      sql("INSERT INTO t (id, ts) VALUES (1, TIMESTAMP'2024-09-01 08:00:00')")
      sql("OPTIMIZE t")

      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("t"))
      assert(ClusteredTableUtils.isSupported(snapshot.protocol),
        "Table protocol should support clustering after creation with CLUSTER BY")
      verifyClusteringColumnsInDomainMetadata(snapshot, Seq("event_date"))
    }
  }
}
