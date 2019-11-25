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

  test("DDL: HiveOnDelta should be a external table ") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        val e = intercept[Exception] {
          runQuery(
            s"""
               |create table deltaTbl(a string, b int)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }.getMessage
        assert(e.contains("HiveOnDelta should be an external table"))
      }
    }
  }

  test("DDL: location should be set when creating table") {
    withTable("deltaTbl") {
      val e = intercept[Exception] {
        runQuery(
          s"""
             |create external table deltaTbl(a string, b int)
             |stored by 'io.delta.hive.DeltaStorageHandler'
         """.stripMargin
        )
      }.getMessage
      assert(e.contains("table location should be set when creating table"))
    }
  }

  test("DDL: HiveOnDelta should not be a partitioned hive table") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        val e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(a string, b int)
               |partitioned by(c string)
               |stored by 'io.delta.hive.DeltaStorageHandler'  location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }.getMessage
        assert(e.contains("HiveOnDelta does not support to create a partition hive table"))
      }
    }
  }

  test("DDL: the delta root path should be existed when create hive table") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        JavaUtils.deleteRecursively(dir)

        val e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(a string, b int)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }.getMessage
        assert(e.contains(s"delta.table.path(${dir.getCanonicalPath}) does not exist..."))
      }
    }
  }

  test("DDL: when creating hive table on a partitioned delta, " +
    "the partition columns should be after data columns") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}"))

        withSparkSession { spark =>
          import spark.implicits._
          testData.toDS.toDF("a", "b").write.format("delta")
            .partitionBy("b").save(dir.getCanonicalPath)
        }

        val e = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(b string, a string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }.getMessage
        assert(e.contains(s"The partition cols of Delta should be after data cols"))
      }
    }
  }

  // check column number & column name
  test("DDL: Hive schema should match delta's schema") {
    withTable("deltaTbl") {
      withTempDir { dir =>
        val testData = (0 until 10).map(x => (x, s"foo${x % 2}", s"test${x % 3}"))

        withSparkSession { spark =>
          import spark.implicits._
          val x = testData.toDS.toDF("a", "b", "c")
          testData.toDS.toDF("a", "b", "c").write.format("delta")
            .partitionBy("b").save(dir.getCanonicalPath)
        }

        // column number mismatch
        val e1 = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(a string, b string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }.getMessage
        assert(e1.contains(s"number does not match"))

        // column name mismatch
        val e2 = intercept[Exception] {
          runQuery(
            s"""
               |create external table deltaTbl(e string, c string, b string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )
        }.getMessage
        assert(e2.contains(s"name does not match"))
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

        assert(runQuery(
          "select * from deltaNonPartitionTbl").sorted ===
          testData.map(r => s"${r._1}\t${r._2}").sorted)
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

        assert(runQuery(
          "select * from deltaPartitionTbl").sorted ===
          testData.map(r => s"${r._1}\t${r._2}").sorted)

        // select partition column order change
        assert(runQuery(
          "select c2, c1 from deltaPartitionTbl").sorted ===
          testData.map(r => s"${r._2}\t${r._1}").sorted)

        assert(runQuery(
          "select c2, c1, c2 as c3 from deltaPartitionTbl").sorted ===
          testData.map(r => s"${r._2}\t${r._1}\t${r._2}").sorted)
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
        assert(runQuery(
          "select * from deltaPartitionTbl where c2 = 'foo0'").sorted ===
          testData.filter(_._2 == "foo0").map(r => s"${r._1}\t${r._2}").sorted)
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
             |create external table deltaPartitionTbl(name string, cnt int, city string, `date` string)
             |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
        )

        // equal pushed down
        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl where `date` = '20180520'")
          .mkString(" ").contains("filterExpr: (date = '20180520')"))
        assert(runQuery(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` = '20180520'")
          .toList.sorted === testData.filter(_._2 == "20180520")
          .map(r => s"${r._1}\t${r._2}\t${r._3}\t${r._4}").sorted)

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl where `date` != '20180520'")
          .mkString(" ").contains("filterExpr: (date <> '20180520')"))
        assert(runQuery(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` != '20180520'")
          .toList.sorted === testData.filter(_._2 != "20180520")
          .map(r => s"${r._1}\t${r._2}\t${r._3}\t${r._4}").sorted)

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl where `date` > '20180520'")
          .mkString(" ").contains("filterExpr: (date > '20180520')"))
        assert(runQuery(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` > '20180520'")
          .toList.sorted === testData.filter(_._2 > "20180520")
          .map(r => s"${r._1}\t${r._2}\t${r._3}\t${r._4}").sorted)

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl where `date` >= '20180520'")
          .mkString(" ").contains("filterExpr: (date >= '20180520')"))
        assert(runQuery(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` >= '20180520'")
          .toList.sorted === testData.filter(_._2 >= "20180520")
          .map(r => s"${r._1}\t${r._2}\t${r._3}\t${r._4}").sorted)

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl where `date` < '20180520'")
          .mkString(" ").contains("filterExpr: (date < '20180520')"))
        assert(runQuery(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` < '20180520'")
          .toList.sorted === testData.filter(_._2 < "20180520")
          .map(r => s"${r._1}\t${r._2}\t${r._3}\t${r._4}").sorted)

        assert(runQuery(
          "explain select city, `date`, name, cnt from deltaPartitionTbl where `date` <= '20180520'")
          .mkString(" ").contains("filterExpr: (date <= '20180520')"))
        assert(runQuery(
          "select city, `date`, name, cnt from deltaPartitionTbl where `date` <= '20180520'")
          .toList.sorted === testData.filter(_._2 <= "20180520")
          .map(r => s"${r._1}\t${r._2}\t${r._3}\t${r._4}").sorted)

        // expr(like) pushed down
        assert(runQuery(
          "explain select * from deltaPartitionTbl where `date` like '201805%'")
          .mkString(" ").contains("filterExpr: (date like '201805%')"))
        assert(runQuery(
          "select * from deltaPartitionTbl where `date` like '201805%'").toList.sorted === testData
          .filter(_._2.contains("201805")).map(r => s"${r._3}\t${r._4}\t${r._1}\t${r._2}").sorted)

        // expr(in) pushed down
        assert(runQuery(
          "explain select name, `date`, cnt from deltaPartitionTbl where `city` in ('hz', 'sz')")
          .mkString(" ").contains("filterExpr: (city) IN ('hz', 'sz')"))
        assert(runQuery(
          "select name, `date`, cnt from deltaPartitionTbl where `city` in ('hz', 'sz')")
          .toList.sorted === testData.filter(c => Seq("hz", "sz").contains(c._1))
          .map(r => s"${r._3}\t${r._2}\t${r._4}").sorted)

        // two partition column pushed down
        assert(runQuery(
          "explain select * from deltaPartitionTbl where `date` = '20181212' and `city` in ('hz', 'sz')")
          .mkString(" ").contains("filterExpr: ((city) IN ('hz', 'sz') and (date = '20181212'))"))
        assert(runQuery(
          "select * from deltaPartitionTbl where `date` = '20181212' and `city` in ('hz', 'sz')")
          .toList.sorted === testData
          .filter(c => Seq("hz", "sz").contains(c._1) && c._2 == "20181212")
          .map(r => s"${r._3}\t${r._4}\t${r._1}\t${r._2}").sorted)

        // data column not be pushed down
        assert(runQuery(
          "explain select * from deltaPartitionTbl where city = 'hz' and name = 'Jim'")
          .mkString(" ").contains("filterExpr: (city = 'hz'"))
        assert(runQuery(
          "select * from deltaPartitionTbl where city = 'hz' and name = 'Jim'")
          .toList.sorted === testData
          .filter(c => c._1 == "hz" && c._3 == "Jim")
          .map(r => s"${r._3}\t${r._4}\t${r._1}\t${r._2}").sorted)
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
               |create external table deltaPartitionTbl(name string, cnt int, city string, `date` string)
               |stored by 'io.delta.hive.DeltaStorageHandler' location '${dir.getCanonicalPath}'
         """.stripMargin
          )

          assert(runQuery(
            "select * from deltaPartitionTbl").toList.sorted === testData1
            .map(r => s"${r._3}\t${r._4}\t${r._1}\t${r._2}").sorted)

          // insert another partition data
          val testData2 = Seq(("bj", "20180520", "Trump", 1))
          testData2.toDS.toDF("city", "date", "name", "cnt").write.mode("append").format("delta")
            .partitionBy("date", "city").save(dir.getCanonicalPath)
          val testData = testData1 ++ testData2
          assert(runQuery(
            "select * from deltaPartitionTbl").toList.sorted === testData
            .map(r => s"${r._3}\t${r._4}\t${r._1}\t${r._2}").sorted)

          // delete one partition
          val deltaTable = DeltaTable.forPath(spark, dir.getCanonicalPath)
          deltaTable.delete("city='hz'")
          assert(runQuery(
            "select * from deltaPartitionTbl").toList.sorted === testData
            .filterNot(_._1 == "hz").map(r => s"${r._3}\t${r._4}\t${r._1}\t${r._2}").sorted)
        }
      }
    }
  }
}