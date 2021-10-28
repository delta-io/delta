/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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
import io.delta.hive.util.JavaUtils

import org.scalatest.BeforeAndAfterEach

abstract class HiveConnectorTest extends HiveTest with BeforeAndAfterEach {

  val hiveGoldenTable = new File(getClass.getResource("/golden/hive").toURI)

  /**
   * Create the full table path for the given golden table and execute the test function.
   *
   * @param name The name of the golden table to load.
   * @param testFunc The test to execute which takes the full table path as input arg.
   */
  def withHiveGoldenTable(name: String)(testFunc: String => Unit): Unit = {
    val tablePath = new File(hiveGoldenTable, name).getCanonicalPath
    testFunc(tablePath)
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
      withHiveGoldenTable("deltatbl-not-allow-write") { tablePath =>

        runQuery(
          s"""
             |CREATE EXTERNAL TABLE deltaTbl(a INT, b STRING)
             |STORED BY 'io.delta.hive.DeltaStorageHandler'
             |LOCATION '${tablePath}'""".stripMargin)
        val e = intercept[Exception] {
          runQuery("INSERT INTO deltaTbl(a, b) VALUES(123, 'foo')")
        }
        if (engine == "tez") {
          // We cannot get the root cause in Tez mode because of HIVE-20974. Currently it's only in
          // the log so we cannot verify it.
          // TODO Remove this `if` branch once we upgrade to a new Hive version containing the fix
          // for HIVE-20974
        } else {
          assert(e.getMessage != null && e.getMessage.contains(
            "Writing to a Delta table in Hive is not supported"))
        }
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
      withHiveGoldenTable("deltatbl-schema-match") { tablePath =>
        // column number mismatch
        var e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(a string, b string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
         """.stripMargin
          )
        }
        assert(e.getMessage.contains(s"schema is not the same"))

        // column name mismatch
        e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(e int, c string, b string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
         """.stripMargin
          )
        }
        assert(e.getMessage.contains(s"schema is not the same"))

        // column order mismatch
        e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(a int, c string, b string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
         """.stripMargin
          )
        }
        assert(e.getMessage.contains(s"schema is not the same"))
      }
    }
  }

//  test("detect schema changes outside Hive") {
//    withTable("deltaTbl") {
//      withTempDir { dir =>
//        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))
//
//        withSparkSession { spark =>
//          import spark.implicits._
//          testData.toDF("a", "b").write.format("delta").save(dir.getCanonicalPath)
//        }
//
//        runQuery(
//          s"""
//             |CREATE EXTERNAL TABLE deltaTbl(a INT, b STRING)
//             |STORED BY 'io.delta.hive.DeltaStorageHandler'
//             |LOCATION '${dir.getCanonicalPath}'""".stripMargin
//        )
//
//        checkAnswer("SELECT * FROM deltaTbl", testData)
//
//        // Change the underlying Delta table to a different schema
//        val testData2 = testData.map(_.swap)
//
//        withSparkSession { spark =>
//          import spark.implicits._
//          testData2.toDF("a", "b")
//            .write
//            .format("delta")
//            .mode("overwrite")
//            .option("overwriteSchema", "true")
//            .save(dir.getCanonicalPath)
//        }
//
//        // Should detect the underlying schema change and fail the query
//        val e = intercept[Exception] {
//          runQuery("SELECT * FROM deltaTbl")
//        }
//        assert(e.getMessage.contains(s"schema is not the same"))
//
//        // Re-create the table because Hive doesn't allow `ALTER TABLE` on a non-native table.
//        // TODO Investigate whether there is a more convenient way to update the table schema.
//        runQuery("DROP TABLE deltaTbl")
//        runQuery(
//          s"""
//             |CREATE EXTERNAL TABLE deltaTbl(a STRING, b INT)
//             |STORED BY 'io.delta.hive.DeltaStorageHandler'
//             |LOCATION '${dir.getCanonicalPath}'""".stripMargin
//        )
//
//        // After fixing the schema, the query should work again.
//        checkAnswer("SELECT * FROM deltaTbl", testData2)
//      }
//    }
//  }

  test("read a non-partitioned table") {
    // Create a Delta table
    withTable("deltaNonPartitionTbl") {
      withHiveGoldenTable("deltatbl-non-partitioned") { tablePath =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        runQuery(
          s"""
             |create external table deltaNonPartitionTbl(c1 int, c2 string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
         """.stripMargin
        )

        checkAnswer("select * from deltaNonPartitionTbl", testData)
      }
    }
  }

  test("read a partitioned table") {
    // Create a Delta table
    withTable("deltaPartitionTbl") {
      withHiveGoldenTable("deltatbl-partitioned") { tablePath =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        runQuery(
          s"""
             |create external table deltaPartitionTbl(c1 int, c2 string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
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

  test("partition prune") {
    withTable("deltaPartitionTbl") {
      withHiveGoldenTable("deltatbl-partition-prune") { tablePath =>
        val testData = Seq(
          ("hz", "20180520", "Jim", 3),
          ("hz", "20180718", "Jone", 7),
          ("bj", "20180520", "Trump", 1),
          ("sh", "20180512", "Jay", 4),
          ("sz", "20181212", "Linda", 8)
        )

        runQuery(
          s"""
             |create external table deltaPartitionTbl(
             |  city string,
             |  `date` string,
             |  name string,
             |  cnt int)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
         """.stripMargin
        )

        // equal pushed down
        checkFilterPushdown(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` = '20180520'",
          "(date = '20180520')",
          testData.filter(_._2 == "20180520"))

        checkFilterPushdown(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` != '20180520'",
          "(date <> '20180520')",
          testData.filter(_._2 != "20180520"))

        checkFilterPushdown(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` > '20180520'",
          "(date > '20180520')",
          testData.filter(_._2 > "20180520"))

        checkFilterPushdown(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` >= '20180520'",
          "(date >= '20180520')",
          testData.filter(_._2 >= "20180520"))

        checkFilterPushdown(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` < '20180520'",
          "(date < '20180520')",
          testData.filter(_._2 < "20180520"))

        checkFilterPushdown(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` <= '20180520'",
          "(date <= '20180520')",
          testData.filter(_._2 <= "20180520"))

        // expr(like) pushed down
        checkFilterPushdown(
          "select * from deltaPartitionTbl where `date` like '201805%'",
          "(date like '201805%')",
          testData.filter(_._2.startsWith("201805")))

        // expr(in) pushed down
        checkFilterPushdown(
          "select name, `date`, cnt from deltaPartitionTbl where `city` in ('hz', 'sz')",
          "(city) IN ('hz', 'sz')",
          testData.filter(c => Seq("hz", "sz").contains(c._1)).map(r => (r._3, r._2, r._4)))

        // two partition column pushed down
        checkFilterPushdown(
          "select * from deltaPartitionTbl where `date` = '20181212' and `city` in ('hz', 'sz')",
          "((city) IN ('hz', 'sz') and (date = '20181212'))",
          testData.filter(c => Seq("hz", "sz").contains(c._1) && c._2 == "20181212"))

        // data column not be pushed down
        checkFilterPushdown(
          "select * from deltaPartitionTbl where city = 'hz' and name = 'Jim'",
          "(city = 'hz')",
          testData.filter(c => c._1 == "hz" && c._3 == "Jim"))
      }
    }
  }

  test("should not touch files not needed when querying a partitioned table") {
    withTable("deltaPartitionTbl") {
      withHiveGoldenTable("deltatbl-touch-files-needed-for-partitioned") { tablePath =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        runQuery(
          s"""
             |create external table deltaPartitionTbl(c1 int, c2 string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
         """.stripMargin
        )

        // Delete the partition not needed in the below query to verify the partition pruning works
        val foo1PartitionFile = new File(tablePath, "c2=foo1")
        assert(foo1PartitionFile.exists())
        JavaUtils.deleteRecursively(foo1PartitionFile)
        checkFilterPushdown(
          "select * from deltaPartitionTbl where c2 = 'foo0'",
          "(c2 = 'foo0')",
          testData.filter(_._2 == "foo0"))
      }
    }
  }

//  test("auto-detected delta partition change") {
//    withTable("deltaPartitionTbl") {
//      withTempDir { dir =>
//        val testData1 = Seq(
//          ("hz", "20180520", "Jim", 3),
//          ("hz", "20180718", "Jone", 7)
//        )
//
//        withSparkSession { spark =>
//          import spark.implicits._
//          testData1.toDS.toDF("city", "date", "name", "cnt").write.format("delta")
//            .partitionBy("date", "city").save(dir.getCanonicalPath)
//
//          runQuery(
//            s"""
//               |create external table deltaPartitionTbl(
//               |  city string,
//               |  `date` string,
//               |  name string,
//               |  cnt int)
//               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
//         """.stripMargin
//          )
//
//          checkAnswer("select * from deltaPartitionTbl", testData1)
//
//          // insert another partition data
//          val testData2 = Seq(("bj", "20180520", "Trump", 1))
//          testData2.toDS.toDF("city", "date", "name", "cnt").write.mode("append").format("delta")
//            .partitionBy("date", "city").save(dir.getCanonicalPath)
//          val testData = testData1 ++ testData2
//          checkAnswer("select * from deltaPartitionTbl", testData)
//
//          // delete one partition
//          val deltaTable = DeltaTable.forPath(spark, dir.getCanonicalPath)
//          deltaTable.delete("city='hz'")
//          checkAnswer("select * from deltaPartitionTbl", testData.filterNot(_._1 == "hz"))
//        }
//      }
//    }
//  }

  test("read a partitioned table that contains special chars in a partition column") {
    withTable("deltaPartitionTbl") {
      withHiveGoldenTable("deltatbl-special-chars-in-partition-column") { tablePath =>
        val testData = (0 until 10).map(x => (x, s"+ =%${x % 2}"))

        runQuery(
          s"""
             |create external table deltaPartitionTbl(c1 int, c2 string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
         """.stripMargin
        )

        checkAnswer("select * from deltaPartitionTbl", testData)
      }
    }
  }

  test("map Spark types to Hive types correctly") {
    withTable("deltaTbl") {
      withHiveGoldenTable("deltatbl-map-types-correctly") { tablePath =>
        runQuery(
          s"""
             |create external table deltaTbl(
             |c1 tinyint, c2 binary, c3 boolean, c4 int, c5 bigint, c6 string, c7 float, c8 double,
             |c9 smallint, c10 date, c11 timestamp, c12 decimal(38, 18), c13 array<string>,
             |c14 map<string, bigint>, c15 struct<f1: string, f2: bigint>)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
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

  test("column names should be case insensitive") {
    // Create a Delta table
    withTable("deltaCaseInsensitiveTest") {
      withHiveGoldenTable("deltatbl-column-names-case-insensitive") { tablePath =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        runQuery(
          s"""
             |create external table deltaCaseInsensitiveTest(fooBar int, Barfoo string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
         """.stripMargin
        )

        checkAnswer("select * from deltaCaseInsensitiveTest", testData)
        for ((col1, col2) <-
               Seq("fooBar" -> "barFoo", "foobar" -> "barfoo", "FOOBAR" -> "BARFOO")) {
          checkAnswer(
            s"select $col1, $col2 from deltaCaseInsensitiveTest",
            testData)
          checkAnswer(
            s"select $col2, $col1 from deltaCaseInsensitiveTest",
            testData.map(_.swap))
          checkAnswer(
            s"select $col1 from deltaCaseInsensitiveTest where $col2 = '2'",
            testData.filter(_._2 == "2").map(x => OneItem(x._1)))
          checkAnswer(
            s"select $col2 from deltaCaseInsensitiveTest where $col1 = 2",
            testData.filter(_._1 == 2).map(x => OneItem(x._2)))
        }
        for (col <- Seq("fooBar", "foobar", "FOOBAR")) {
          checkAnswer(
            s"select $col from deltaCaseInsensitiveTest",
            testData.map(x => OneItem(x._1)))
        }
        for (col <- Seq("barFoo", "barfoo", "BARFOO")) {
          checkAnswer(
            s"select $col from deltaCaseInsensitiveTest",
            testData.map(x => OneItem(x._2)))
        }
      }
    }
  }

  test("fail the query when the path is deleted after the table is created") {
    withTable("deltaTbl") {
      withHiveGoldenTable("deltatbl-deleted-path") { tablePath =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        runQuery(
          s"""
             |create external table deltaTbl(c1 int, c2 string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${tablePath}'
         """.stripMargin
        )

        checkAnswer("select * from deltaTbl", testData)

        JavaUtils.deleteRecursively(new File(tablePath))

        val e = intercept[Exception] {
          checkAnswer("select * from deltaTbl", testData)
        }
        assert(e.getMessage.contains("not a Delta table"))
      }
    }
  }

  test("fail incorrect format config") {
    val formatKey = engine match {
      case "mr" => "hive.input.format"
      case "tez" => "hive.tez.input.format"
      case other => throw new UnsupportedOperationException(s"Unsupported engine: $other")
    }
    withHiveGoldenTable("deltatbl-incorrect-format-config") { tablePath =>
      withTable("deltaTbl") {

        runQuery(
          s"""
             |CREATE EXTERNAL TABLE deltaTbl(a INT, b STRING)
             |STORED BY 'io.delta.hive.DeltaStorageHandler'
             |LOCATION '${tablePath}'""".stripMargin)

        withHiveConf(formatKey, "org.apache.hadoop.hive.ql.io.HiveInputFormat") {
          val e = intercept[Exception] {
            runQuery("SELECT * from deltaTbl")
          }
          assert(e.getMessage.contains(formatKey))
          assert(e.getMessage.contains(classOf[HiveInputFormat].getName))
        }
      }
    }
  }
}

case class OneItem[T](t: T)
