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

package org.apache.spark.sql.delta

import java.io.File

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.util.FileNames.deltaFile

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

trait DeltaProtocolVersionSuiteBase extends QueryTest
    with SharedSparkSession {

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
        Iterator(Protocol(Integer.MAX_VALUE, Integer.MAX_VALUE).json))
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
}

class DeltaProtocolVersionSuite extends DeltaProtocolVersionSuiteBase
