/*
 * Copyright (2020) The Delta Lake Project Authors.
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

// scalastyle:off import.ordering.noEmptyLine
import java.io.File

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames.deltaFile

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

trait DeltaProtocolVersionSuiteBase extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private lazy val testTableSchema = spark.range(1).schema

  // This is solely a test hook. Users cannot create new Delta tables with protocol lower than
  // that of their current version.
  protected def createTableWithProtocol(
      protocol: Protocol,
      path: File,
      schema: StructType = testTableSchema): DeltaLog = {
    val log = DeltaLog.forTable(spark, path)
    log.ensureLogDirectoryExist()
    log.store.write(
      deltaFile(log.logPath, 0),
      Iterator(Metadata(schemaString = schema.json).json, protocol.json))
    log.update()
    log
  }

  test("upgrade to current version") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(1, 1), path)
      assert(log.snapshot.protocol == Protocol(1, 1))
      log.upgradeProtocol()
      assert(log.snapshot.protocol == Protocol())
    }
  }

  test("overwrite keeps the same protocol version") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(0, 0), path)
      spark.range(1)
        .write
        .format("delta")
        .mode("overwrite")
        .save(path.getCanonicalPath)
      log.update()
      assert(log.snapshot.protocol == Protocol(0, 0))
    }
  }

  test("access with protocol too high") {
    withTempDir { path =>
      val log = DeltaLog.forTable(spark, path)
      log.ensureLogDirectoryExist()
      log.store.write(
        deltaFile(log.logPath, 0),
        Iterator(Metadata().json, Protocol(Integer.MAX_VALUE, Integer.MAX_VALUE).json))
      intercept[InvalidProtocolVersionException] {
        spark.range(1).write.format("delta").save(path.getCanonicalPath)
      }
    }
  }

  test("can't downgrade") {
    withTempDir { path =>
      val log = DeltaLog.forTable(spark, path)
      assert(log.snapshot.protocol == Protocol())
      intercept[ProtocolDowngradeException] {
        log.upgradeProtocol(Protocol(0, 0))
      }
    }
  }

  test("concurrent upgrade") {
    withTempDir { path =>
      val newProtocol = Protocol()
      val log = createTableWithProtocol(Protocol(0, 0), path)

      // We have to copy out the internals of upgradeProtocol to induce the concurrency.
      val txn = log.startTransaction()
      log.upgradeProtocol(newProtocol)
      intercept[ProtocolChangedException] {
        txn.commit(Seq(newProtocol), DeltaOperations.UpgradeProtocol(newProtocol))
      }
    }
  }

  test("incompatible protocol change during the transaction") {
    for (incompatibleProtocol <- Seq(
      Protocol(minReaderVersion = Int.MaxValue),
      Protocol(minWriterVersion = Int.MaxValue),
      Protocol(minReaderVersion = Int.MaxValue, minWriterVersion = Int.MaxValue)
    )) {
      withTempDir { path =>
        spark.range(0).write.format("delta").save(path.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(spark, path)
        val txn = deltaLog.startTransaction()
        val currentVersion = txn.snapshot.version
        deltaLog.store.write(
          deltaFile(deltaLog.logPath, currentVersion + 1),
          Iterator(incompatibleProtocol.json))

        // Should detect the above incompatible protocol change and fail
        intercept[InvalidProtocolVersionException] {
          txn.commit(AddFile("test", Map.empty, 1, 1, dataChange = true) :: Nil, ManualUpdate)
        }
        // Make sure we didn't commit anything
        val p = deltaFile(deltaLog.logPath, currentVersion + 2)
        assert(
          !p.getFileSystem(spark.sessionState.newHadoopConf).exists(p),
          s"$p should not be committed")
      }
    }
  }

  import testImplicits._
  /** Creates a Delta table and checks the expected protocol version */
  private def testCreation(tableName: String, writerVersion: Int)(fn: String => Unit): Unit = {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
        withTable(tableName) {
          fn(dir.getCanonicalPath)

          val deltaLog = DeltaLog.forTable(spark, dir)
          assert(deltaLog.snapshot.version === 0, "did not create a Delta table")
          assert(deltaLog.snapshot.protocol.minWriterVersion === writerVersion)
          assert(deltaLog.snapshot.protocol.minReaderVersion === 1)
        }
      }
    }
  }

  test("creating a new table with default protocol") {
    val tableName = "delta_test"

    def testTableCreation(fn: String => Unit): Unit = {
      testCreation(tableName, 1) { dir =>
        fn(dir)
        val e = intercept[AnalysisException] {
          sql(s"ALTER TABLE delta.`$dir` SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
        }
        assert(e.getMessage.contains("protocol version"))
        assert(e.getMessage.contains("delta.appendOnly"))
      }
    }

    testTableCreation { dir => spark.range(10).write.format("delta").save(dir) }
    testTableCreation { dir =>
      spark.range(10).write.format("delta").option("path", dir).saveAsTable(tableName)
    }
    testTableCreation { dir =>
      spark.range(10).writeTo(tableName).using("delta").tableProperty("location", dir).create()
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName (id bigint) USING delta LOCATION '$dir'")
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName USING delta LOCATION '$dir' AS SELECT * FROM range(10)")
    }
    testTableCreation { dir =>
      val stream = MemoryStream[Int]
      stream.addData(1 to 10)
      val q = stream.toDF().writeStream.format("delta")
        .option("checkpointLocation", new File(dir, "_checkpoint").getCanonicalPath)
        .start(dir)
      q.processAllAvailable()
      q.stop()
    }

    testTableCreation { dir =>
      spark.range(10).write.mode("append").parquet(dir)
      sql(s"CONVERT TO DELTA parquet.`$dir`")
    }
  }

  test(
    "creating a new table with default protocol - requiring more recent protocol version") {
    val tableName = "delta_test"
    def testTableCreation(fn: String => Unit): Unit = testCreation(tableName, 2)(fn)

    testTableCreation { dir =>
      spark.range(10).writeTo(tableName).using("delta")
        .tableProperty("location", dir)
        .tableProperty("delta.appendOnly", "true")
        .create()
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName (id bigint) USING delta LOCATION '$dir' " +
        s"TBLPROPERTIES (delta.appendOnly = 'true')")
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName USING delta TBLPROPERTIES (delta.appendOnly = 'true') " +
        s"LOCATION '$dir' AS SELECT * FROM range(10)")
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName (id bigint NOT NULL) USING delta LOCATION '$dir'")
    }

    withSQLConf("spark.databricks.delta.properties.defaults.appendOnly" -> "true") {
      testTableCreation { dir => spark.range(10).write.format("delta").save(dir) }
      testTableCreation { dir =>
        spark.range(10).write.format("delta").option("path", dir).saveAsTable(tableName)
      }
      testTableCreation { dir =>
        spark.range(10).writeTo(tableName).using("delta").tableProperty("location", dir).create()
      }
      testTableCreation { dir =>
        sql(s"CREATE TABLE $tableName (id bigint) USING delta LOCATION '$dir'")
      }
      testTableCreation { dir =>
        sql(s"CREATE TABLE $tableName USING delta LOCATION '$dir' AS SELECT * FROM range(10)")
      }
      testTableCreation { dir =>
        val stream = MemoryStream[Int]
        stream.addData(1 to 10)
        val q = stream.toDF().writeStream.format("delta")
          .option("checkpointLocation", new File(dir, "_checkpoint").getCanonicalPath)
          .start(dir)
        q.processAllAvailable()
        q.stop()
      }

      testTableCreation { dir =>
        spark.range(10).write.mode("append").parquet(dir)
        sql(s"CONVERT TO DELTA parquet.`$dir`")
      }
    }
  }

  test("replacing a new table with default protocol") {
    withTempDir { dir =>
      // In this test we go back and forth through protocol versions, testing the various syntaxes
      // of replacing tables
      val tbl = "delta_test"
      withTable(tbl) {
        withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
          sql(s"CREATE TABLE $tbl (id bigint) USING delta LOCATION '${dir.getCanonicalPath}'")
        }
        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.snapshot.protocol.minWriterVersion === 1,
          "Should've picked up the protocol from the configuration")

        // Replace the table and make sure the config is picked up
        withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "2") {
          spark.range(10).writeTo(tbl).using("delta")
            .tableProperty("location", dir.getCanonicalPath).replace()
        }
        assert(deltaLog.snapshot.protocol.minWriterVersion === 2,
          "Should've picked up the protocol from the configuration")

        // Replace with the old writer again
        withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
          sql(s"REPLACE TABLE $tbl (id bigint) USING delta LOCATION '${dir.getCanonicalPath}'")
          assert(deltaLog.snapshot.protocol.minWriterVersion === 1,
            "Should've created a new protocol")

          sql(s"CREATE OR REPLACE TABLE $tbl (id bigint NOT NULL) USING delta " +
            s"LOCATION '${dir.getCanonicalPath}'")
          assert(deltaLog.snapshot.protocol.minWriterVersion === 2,
            "Invariant should require the higher protocol")

          // Go back to version 1
          sql(s"REPLACE TABLE $tbl (id bigint) USING delta LOCATION '${dir.getCanonicalPath}'")
          assert(deltaLog.snapshot.protocol.minWriterVersion === 1,
            "Should've created a new protocol")

          // Check table properties with different syntax
          spark.range(10).writeTo(tbl).tableProperty("location", dir.getCanonicalPath)
            .tableProperty("delta.appendOnly", "true").using("delta").createOrReplace()
          assert(deltaLog.snapshot.protocol.minWriterVersion === 2,
            "appendOnly should require the higher protocol")
        }
      }
    }
  }

  test("bad inputs for default protocol versions") {
    withTempDir { path =>
      val dir = path.getCanonicalPath
      Seq("abc", "", "0", (Action.readerVersion + 1).toString).foreach { conf =>
        val e = intercept[IllegalArgumentException] {
          withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> conf) {
            spark.range(10).write.format("delta").save(dir)
          }
        }
      }
      Seq("abc", "", "0", (Action.writerVersion + 1).toString).foreach { conf =>
        intercept[IllegalArgumentException] {
          withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> conf) {
            spark.range(10).write.format("delta").save(dir)
          }
        }
      }
    }
  }
}

class DeltaProtocolVersionSuite extends DeltaProtocolVersionSuiteBase
