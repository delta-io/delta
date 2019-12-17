/*
 * Copyright 2019 Databricks, Inc.
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

package io.delta.hive

import java.io.File

import io.delta.hive.test.HiveTest
import io.delta.tables.DeltaTable

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.BeforeAndAfterEach

class HiveConnectorSuite extends HiveTest with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    DeltaLog.clearCache()
  }

  override def afterEach(): Unit = {
    DeltaLog.clearCache()
  }

  test("should not allow to create a non external Delta table") {
    val e = intercept[Exception] {
      runQuery(
        s"""
           |create table deltaTbl(a string, b int)
           |stored by 'io.delta.hive.DeltaStorageHandler'""".stripMargin
      )
    }
    assert(e.getMessage != null && e.getMessage.contains("Only external Delta tables"))
  }

  test("location should be set when creating table") {
    withTable("deltaTbl") {
      val e = intercept[Exception] {
        runQuery(
          s"""
             |create external table deltaTbl(a string, b int)
             |stored by 'io.delta.hive.DeltaStorageHandler'
         """.stripMargin
        )
      }
      assert(e.getMessage.contains("table location should be set"))
    }
  }

  test("should not allow to specify partition columns") {
    withTempDir { dir =>
      val e = intercept[Exception] {
        runQuery(
          s"""
             |CREATE EXTERNAL TABLE deltaTbl(a STRING, b INT)
             |PARTITIONED BY(c STRING)
             |STORED BY 'io.delta.hive.DeltaStorageHandler'
             |LOCATION '${dir.getCanonicalPath}' """.stripMargin)
      }
      assert(e.getMessage != null && e.getMessage.matches(
        "(?s).*partition columns.*should not be set manually.*"))
    }
  }

  test("should not allow to write to a Delta table") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        withSparkSession { spark =>
          import spark.implicits._
          val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))
          testData.toDS.toDF("a", "b").write.format("delta").save(dir.getCanonicalPath)
        }

        runQuery(
          s"""
             |CREATE EXTERNAL TABLE deltaTbl(a INT, b STRING)
             |STORED BY 'io.delta.hive.DeltaStorageHandler'
             |LOCATION '${dir.getCanonicalPath}'""".stripMargin)
        val e = intercept[Exception] {
          runQuery("INSERT INTO deltaTbl(a, b) VALUES(123, 'foo')")
        }
        assert(e.getMessage != null && e.getMessage.contains(
          "Writing to a Delta table in Hive is not supported"))
      }
    }
  }

  test("the table path should point to a Delta table") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        // path exists but is not a Delta table should fail
        assert(dir.exists())
        var e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(a string, b int)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }
        assert(e.getMessage.contains("not a Delta table"))

        // path doesn't exist should fail as well
        JavaUtils.deleteRecursively(dir)
        assert(!dir.exists())
        e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(a string, b int)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }
        assert(e.getMessage.contains("does not exist"))
      }
    }
  }

  test("Hive schema should match delta's schema") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}", s"test${x % 3}"))

        withSparkSession { spark =>
          import spark.implicits._
          testData.toDS.toDF("a", "b", "c").write.format("delta")
            .partitionBy("b").save(dir.getCanonicalPath)
        }

        // column number mismatch
        var e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(a string, b string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }
        assert(e.getMessage.contains(s"schema is not the same"))

        // column name mismatch
        e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(e int, c string, b string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }
        assert(e.getMessage.contains(s"schema is not the same"))

        // column order mismatch
        e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(a int, c string, b string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }
        assert(e.getMessage.contains(s"schema is not the same"))
      }
    }
  }

  test("detect schema changes outside Hive") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        withSparkSession { spark =>
          import spark.implicits._
          testData.toDF("a", "b").write.format("delta").save(dir.getCanonicalPath)
        }

        runQuery(
          s"""
             |CREATE EXTERNAL TABLE deltaTbl(a INT, b STRING)
             |STORED BY 'io.delta.hive.DeltaStorageHandler'
             |LOCATION '${dir.getCanonicalPath}'""".stripMargin
        )

        checkAnswer("SELECT * FROM deltaTbl", testData)

        // Change the underlying Delta table to a different schema
        val testData2 = testData.map(_.swap)

        withSparkSession { spark =>
          import spark.implicits._
          testData2.toDF("a", "b")
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(dir.getCanonicalPath)
        }

        // Should detect the underlying schema change and fail the query
        val e = intercept[Exception] {
          runQuery("SELECT * FROM deltaTbl")
        }
        assert(e.getMessage.contains(s"schema is not the same"))

        // Re-create the table because Hive doesn't allow `ALTER TABLE` on a non-native table.
        // TODO Investigate whether there is a more convenient way to update the table schema.
        runQuery("DROP TABLE deltaTbl")
        runQuery(
          s"""
             |CREATE EXTERNAL TABLE deltaTbl(a STRING, b INT)
             |STORED BY 'io.delta.hive.DeltaStorageHandler'
             |LOCATION '${dir.getCanonicalPath}'""".stripMargin
        )

        // After fixing the schema, the query should work again.
        checkAnswer("SELECT * FROM deltaTbl", testData2)
      }
    }
  }

  test("read a non-partitioned table") {
    // Create a Delta table
    withTable("deltaNonPartitionTbl") {
      withTempDir { dir =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        withSparkSession{ spark =>
          import spark.implicits._
          testData.toDS.toDF("c1", "c2").write.format("delta").save(dir.getCanonicalPath)
        }

        runQuery(
          s"""
             |create external table deltaNonPartitionTbl(c1 int, c2 string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
        )

        checkAnswer("select * from deltaNonPartitionTbl", testData)
      }
    }
  }

  test("read a partitioned table") {
    // Create a Delta table
    withTable("deltaPartitionTbl") {
      withTempDir { dir =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        withSparkSession { spark =>
          import spark.implicits._
          testData.toDS.toDF("c1", "c2").write.format("delta")
            .partitionBy("c2").save(dir.getCanonicalPath)
        }

        runQuery(
          s"""
             |create external table deltaPartitionTbl(c1 int, c2 string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
        )

        checkAnswer("select * from deltaPartitionTbl", testData)

        // select partition column order change
        checkAnswer("select c2, c1 from deltaPartitionTbl", testData.map(_.swap))

        checkAnswer(
          "select c2, c1, c2 as c3 from deltaPartitionTbl",
          testData.map(r => (r._2, r._1, r._2)))
      }
    }
  }

  test("read a partitioned table with a partition filter") {
    // Create a Delta table
    withTable("deltaPartitionTbl") {
      withTempDir { dir =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        withSparkSession { spark =>
          import spark.implicits._
          testData.toDS.toDF("c1", "c2").write.format("delta")
            .partitionBy("c2").save(dir.getCanonicalPath)
        }

        runQuery(
          s"""
             |create external table deltaPartitionTbl(c1 int, c2 string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
        )

        // Delete the partition not needed in the below query to verify the partition pruning works
        JavaUtils.deleteRecursively(new File(dir, "c2=foo1"))
        assert(dir.listFiles.map(_.getName).sorted === Seq("_delta_log", "c2=foo0").sorted)
        checkAnswer(
          "select * from deltaPartitionTbl where c2 = 'foo0'",
          testData.filter(_._2 == "foo0"))
      }
    }
  }

  test("partition prune") {
    withTable("deltaPartitionTbl") {
      withTempDir { dir =>
        val testData = Seq(
          ("hz", "20180520", "Jim", 3),
          ("hz", "20180718", "Jone", 7),
          ("bj", "20180520", "Trump", 1),
          ("sh", "20180512", "Jay", 4),
          ("sz", "20181212", "Linda", 8)
        )

        withSparkSession { spark =>
          import spark.implicits._
          testData.toDS.toDF("city", "date", "name", "cnt").write.format("delta")
            .partitionBy("date", "city").save(dir.getCanonicalPath)
        }

        runQuery(
          s"""
             |create external table deltaPartitionTbl(
             |  city string,
             |  `date` string,
             |  name string,
             |  cnt int)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
        )

        // equal pushed down
        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl where `date` = '20180520'")
          .mkString(" ").contains("filterExpr: (date = '20180520')"))
        checkAnswer(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` = '20180520'",
          testData.filter(_._2 == "20180520"))

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl " +
            "where `date` != '20180520'")
          .mkString(" ").contains("filterExpr: (date <> '20180520')"))
        checkAnswer(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` != '20180520'",
          testData.filter(_._2 != "20180520"))

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl where `date` > '20180520'")
          .mkString(" ").contains("filterExpr: (date > '20180520')"))
        checkAnswer(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` > '20180520'",
          testData.filter(_._2 > "20180520"))

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl " +
            "where `date` >= '20180520'")
          .mkString(" ").contains("filterExpr: (date >= '20180520')"))
        checkAnswer(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` >= '20180520'",
          testData.filter(_._2 >= "20180520"))

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl where `date` < '20180520'")
          .mkString(" ").contains("filterExpr: (date < '20180520')"))
        checkAnswer(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` < '20180520'",
          testData.filter(_._2 < "20180520"))

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl " +
            "where `date` <= '20180520'")
          .mkString(" ").contains("filterExpr: (date <= '20180520')"))
        checkAnswer(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` <= '20180520'",
          testData.filter(_._2 <= "20180520"))

        // expr(like) pushed down
        assert(runQuery(
          "explain select * from deltaPartitionTbl where `date` like '201805%'")
          .mkString(" ").contains("filterExpr: (date like '201805%')"))
        checkAnswer(
          "select * from deltaPartitionTbl where `date` like '201805%'",
          testData.filter(_._2.contains("201805")))

        // expr(in) pushed down
        assert(runQuery(
          "explain select name, `date`, cnt from deltaPartitionTbl where `city` in ('hz', 'sz')")
          .mkString(" ").contains("filterExpr: (city) IN ('hz', 'sz')"))
        checkAnswer(
          "select name, `date`, cnt from deltaPartitionTbl where `city` in ('hz', 'sz')",
          testData.filter(c => Seq("hz", "sz").contains(c._1)).map(r => (r._3, r._2, r._4)))

        // two partition column pushed down
        assert(runQuery(
          "explain select * from deltaPartitionTbl " +
            "where `date` = '20181212' and `city` in ('hz', 'sz')")
          .mkString(" ").contains("filterExpr: ((city) IN ('hz', 'sz') and (date = '20181212'))"))
        checkAnswer(
          "select * from deltaPartitionTbl where `date` = '20181212' and `city` in ('hz', 'sz')",
          testData.filter(c => Seq("hz", "sz").contains(c._1) && c._2 == "20181212"))

        // data column not be pushed down
        assert(runQuery(
          "explain select * from deltaPartitionTbl where city = 'hz' and name = 'Jim'")
          .mkString(" ").contains("filterExpr: (city = 'hz'"))
        checkAnswer(
          "select * from deltaPartitionTbl where city = 'hz' and name = 'Jim'",
          testData.filter(c => c._1 == "hz" && c._3 == "Jim"))
      }
    }
  }

  test("auto-detected delta partition change") {
    withTable("deltaPartitionTbl") {
      withTempDir { dir =>
        val testData1 = Seq(
          ("hz", "20180520", "Jim", 3),
          ("hz", "20180718", "Jone", 7)
        )

        withSparkSession { spark =>
          import spark.implicits._
          testData1.toDS.toDF("city", "date", "name", "cnt").write.format("delta")
            .partitionBy("date", "city").save(dir.getCanonicalPath)

          runQuery(
            s"""
               |create external table deltaPartitionTbl(
               |  city string,
               |  `date` string,
               |  name string,
               |  cnt int)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )

          checkAnswer("select * from deltaPartitionTbl", testData1)

          // insert another partition data
          val testData2 = Seq(("bj", "20180520", "Trump", 1))
          testData2.toDS.toDF("city", "date", "name", "cnt").write.mode("append").format("delta")
            .partitionBy("date", "city").save(dir.getCanonicalPath)
          val testData = testData1 ++ testData2
          checkAnswer("select * from deltaPartitionTbl", testData)

          // delete one partition
          val deltaTable = DeltaTable.forPath(spark, dir.getCanonicalPath)
          deltaTable.delete("city='hz'")
          checkAnswer("select * from deltaPartitionTbl", testData.filterNot(_._1 == "hz"))
        }
      }
    }
  }

  test("read a partitioned table that contains special chars in a partition column") {
    withTable("deltaPartitionTbl") {
      withTempDir { dir =>
        val testData = (0 until 10).map(x => (x, s"+ =%${x % 2}"))

        withSparkSession { spark =>
          import spark.implicits._
          testData.toDS.toDF("c1", "c2").write.format("delta")
            .partitionBy("c2").save(dir.getCanonicalPath)
        }

        runQuery(
          s"""
             |create external table deltaPartitionTbl(c1 int, c2 string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
        )

        checkAnswer("select * from deltaPartitionTbl", testData)
      }
    }
  }

  test("map Spark types to Hive types correctly") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        val testData = Seq(
          TestClass(
            97.toByte,
            Array(98.toByte, 99.toByte),
            true,
            4,
            5L,
            "foo",
            6.0f,
            7.0,
            8.toShort,
            new java.sql.Date(60000000L),
            new java.sql.Timestamp(60000000L),
            new java.math.BigDecimal(12345.6789),
            Array("foo", "bar"),
            Map("foo" -> 123L),
            TestStruct("foo", 456L)
          )
        )

        withSparkSession { spark =>
          import spark.implicits._
          testData.toDF.write.format("delta").save(dir.getCanonicalPath)
        }

        runQuery(
          s"""
             |create external table deltaTbl(
             |c1 tinyint, c2 binary, c3 boolean, c4 int, c5 bigint, c6 string, c7 float, c8 double,
             |c9 smallint, c10 date, c11 timestamp, c12 decimal(38, 18), c13 array<string>,
             |c14 map<string, bigint>, c15 struct<f1: string, f2: bigint>)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
        )

        val expected = (
          "97",
          "bc",
          "true",
          "4",
          "5",
          "foo",
          "6.0",
          "7.0",
          "8",
          "1970-01-01",
          "1970-01-01 08:40:00",
          "12345.678900000000794535",
          """["foo","bar"]""",
          """{"foo":123}""",
          """{"f1":"foo","f2":456}"""
        )
        checkAnswer("select * from deltaTbl", Seq(expected))
      }
    }
  }
}

case class TestStruct(f1: String, f2: Long)

/** A special test class that covers all Spark types we support in the Hive connector. */
case class TestClass(
  c1: Byte,
  c2: Array[Byte],
  c3: Boolean,
  c4: Int,
  c5: Long,
  c6: String,
  c7: Float,
  c8: Double,
  c9: Short,
  c10: java.sql.Date,
  c11: java.sql.Timestamp,
  c12: BigDecimal,
  c13: Array[String],
  c14: Map[String, Long],
  c15: TestStruct
)
