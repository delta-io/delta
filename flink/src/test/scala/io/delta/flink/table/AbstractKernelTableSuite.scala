/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.table

import java.net.URI
import java.nio.file.AccessDeniedException
import java.util.{ConcurrentModificationException, Optional, UUID}

import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.flink.table.SnapshotCacheManager.NoCacheManager
import io.delta.kernel.Snapshot
import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{AddFile, SingleAction}
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator, DataFileStatus}

import org.apache.flink.util.InstantiationUtil
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class AbstractKernelTableSuite extends AnyFunSuite with TestHelper {

  test("normalize URI") {
    assert(AbstractKernelTable.normalize(URI.create("file:/var/char/good")).toString ==
      "file:///var/char/good/")
    assert(AbstractKernelTable.normalize(URI.create("/var/char/good")).toString ==
      "file:///var/char/good/")
    assert(AbstractKernelTable.normalize(URI.create("file:///var/char/good")).toString ==
      "file:///var/char/good/")
    assert(AbstractKernelTable.normalize(URI.create("s3://host/var")).toString == "s3://host/var/")
  }

  test("table is serializable") {
    withTempDir { dir =>
      val tablePath = dir.getPath
      val schema = new StructType().add("id", IntegerType.INTEGER)

      val table = new HadoopTableForTest(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        Seq.empty[String].asJava)
      table.open()

      val serialized: Array[Byte] = InstantiationUtil.serializeObject(table)
      val copy = InstantiationUtil.deserializeObject(serialized, getClass.getClassLoader)
        .asInstanceOf[AbstractKernelTable]
      assert(copy != null)
    }
  }

  test("commit to empty table without partition") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTableForTest(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List.empty[String].asJava)
      table.open()

      val actions = (0 until 5).map { i =>
        dummyAddFileRow(schema, 10 + i, Map("part" -> Literal.ofString("p" + i)))
      }.toList.asJava
      val dataActions = new CloseableIterable[Row]() {
        override def iterator: CloseableIterator[Row] =
          Utils.toCloseableIterator(actions.iterator())
        override def close(): Unit = {
          // Nothing to close
        }
      }
      table.commit(dataActions, "a", 100, Map.empty[String, String].asJava)

      // The target table should have one version
      verifyTableContent(
        dir.toString,
        (version, actions, _) => {
          assert(0L == version)
          // There should be 5 files to scan
          assert(5 == actions.size)
          assert(60 == actions.map(_.getNumRecords.get.longValue()).sum)
        })
    }
  }

  test("commit to empty table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTableForTest(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      val actions = (0 until 5).map { i =>
        dummyAddFileRow(schema, 10 + i, Map("part" -> Literal.ofString("p" + i)))
      }.toList.asJava
      val dataActions = new CloseableIterable[Row]() {
        override def iterator: CloseableIterator[Row] =
          Utils.toCloseableIterator(actions.iterator())
        override def close(): Unit = {
          // Nothing to close
        }
      }
      table.commit(dataActions, "a", 100, Map.empty[String, String].asJava)

      // The target table should have one version
      verifyTableContent(
        dir.toString,
        (version, actions, _) => {
          assert(0L == version)
          // There should be 5 files to scan
          assert(5 == actions.size)
          assert(Set("p0", "p1", "p2", "p3", "p4") ==
            actions.map(_.getPartitionValues.getValues.getString(0)).toSet)
          assert(60 == actions.map(_.getNumRecords.get.longValue()).sum)
        })
    }
  }

  test("commit to existing table without partition") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTableForTest(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List.empty[String].asJava)
      table.open()

      createNonEmptyTable(
        DefaultEngine.create(new Configuration()),
        tablePath,
        schema,
        Seq(),
        30)

      val actions = (0 until 5).map { i =>
        dummyAddFileRow(schema, 10 + i, Map.empty[String, Literal])
      }.toList.asJava
      val dataActions = new CloseableIterable[Row]() {
        override def iterator: CloseableIterator[Row] =
          Utils.toCloseableIterator(actions.iterator())
        override def close(): Unit = {
          // Nothing to close
        }
      }
      table.commit(dataActions, "a", 100, Map.empty[String, String].asJava)

      // The target table should have one version
      verifyTableContent(
        dir.toString,
        (version, actions, _) => {
          assert(1L == version)
          // There should be 6 files to scan
          assert(6 == actions.size)
          assert(90 == actions.map(_.getNumRecords.get.longValue()).sum)
        })
    }
  }

  test("commit to existing table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTableForTest(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      createNonEmptyTable(
        DefaultEngine.create(new Configuration()),
        tablePath,
        schema,
        Seq("part"),
        30)

      val actions = (0 until 5).map { i =>
        dummyAddFileRow(schema, 10 + i, Map("part" -> Literal.ofString("p" + i)))
      }.toList.asJava
      val dataActions = new CloseableIterable[Row]() {
        override def iterator: CloseableIterator[Row] =
          Utils.toCloseableIterator(actions.iterator())
        override def close(): Unit = {
          // Nothing to close
        }
      }
      table.commit(dataActions, "a", 100, Map.empty[String, String].asJava)

      // The target table should have one version
      verifyTableContent(
        dir.toString,
        (version, actions, _) => {
          assert(1L == version)
          // There should be 6 files to scan
          assert(6 == actions.size)
          val parts = actions.map(_.getPartitionValues.getValues.getString(0)).toSet
          assert(Set("p0", "p1", "p2", "p3", "p4").forall(parts.contains(_)))
          assert(90 == actions.map(_.getNumRecords.get.longValue()).sum)
        })
    }
  }

  test("refresh on empty table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTableForTest(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      table.refresh()

      assert(table.snapshot().isEmpty)
    }
  }

  test("refresh on existing table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTableForTest(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      createNonEmptyTable(
        DefaultEngine.create(new Configuration()),
        tablePath,
        schema,
        Seq("part"),
        30)

      table.refresh()
      assert(0 == table.snapshot().get().getVersion)
    }
  }

  test("close cancel ongoing operations") {
    withTempDir { dir =>
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING)
      createNonEmptyTable(engine, dir.getAbsolutePath, schema)

      var callcounter = 0

      val table = new HadoopTableForTest(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        Seq.empty[String].asJava) {

        override protected def loadLatestSnapshot(): Snapshot = {
          val snapshot = super.loadLatestSnapshot()
          callcounter += 1
          if (callcounter >= 2) {
            for (i <- 0 until 50) {
              Thread.sleep(100);
            }
          }
          snapshot
        }
      }
      table.open()

      // With cache, load will not be called again
      table.setCacheManager(new NoCacheManager)

      // this thread will refresh the table
      val thread1 = new Thread(() => {
        table.refresh()
      })
      thread1.start()

      // If we do not call close, the refresh will take ~5s to stop
      var wcstart = System.currentTimeMillis()
      while (thread1.isAlive) {
        Thread.sleep(100)
      }
      var elapse = System.currentTimeMillis() - wcstart
      assert(elapse >= 4500)

      // this thread will refresh the table
      val thread2 = new Thread(() => {
        try {
          table.refresh()
        } catch {
          case _: Exception => // Ignore the InterruptException
        }
      })
      thread2.start()
      // If we call close, the refresh was interrupted quickly
      Thread.sleep(100)
      wcstart = System.currentTimeMillis()
      table.close()
      while (thread2.isAlive) {
        Thread.sleep(100)
      }
      elapse = System.currentTimeMillis() - wcstart
      assert(elapse < 200)
    }
  }

  test("retry concurrency exception") {
    withTempDir(dir => {
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING)
      createNonEmptyTable(engine, dir.getAbsolutePath, schema)

      var retryCounter = 0
      var loadCounter = 0
      val testHadoopTable = new HadoopTableForTest(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        List.empty[String].asJava) {

        override protected def loadLatestSnapshot(): Snapshot = {
          loadCounter += 1
          val result = super.loadLatestSnapshot()
          if (loadCounter == 2) {
            throw new ConcurrentModificationException()
          }
          result
        }

        override def reloadSnapshot() {
          // This should be called once
          retryCounter += 1
        }
      }
      testHadoopTable.open()
      // Disable cache for retry to work
      testHadoopTable.setCacheManager(new NoCacheManager)

      // This will be retried once
      val dummyAddFile = AddFile.convertDataFileStatus(
        schema,
        URI.create(dir.getAbsolutePath),
        new DataFileStatus(UUID.randomUUID().toString, 1000L, 2000L, Optional.empty),
        Map.empty[String, Literal].asJava,
        true,
        Map.empty[String, String].asJava,
        Optional.empty(),
        Optional.empty(),
        Optional.empty())
      testHadoopTable.commit(
        CloseableIterable
          .inMemoryIterable(Utils.singletonCloseableIterator(
            SingleAction.createAddFileSingleAction(dummyAddFile.toRow))),
        "a",
        1000L,
        Map.empty[String, String].asJava)

      assert(retryCounter == 1)
      assert(loadCounter == 3)
    })
  }

  test("retry credential exception to succeed") {
    withTempDir(dir => {
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING)
      createNonEmptyTable(engine, dir.getAbsolutePath, schema)

      var retryCounter = 0
      var loadCounter = 0
      // Release the lock after write a new version to the table
      val testHadoopTable = new HadoopTableForTest(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        List.empty[String].asJava) {

        override protected def loadLatestSnapshot(): Snapshot = {
          loadCounter += 1
          val result = super.loadLatestSnapshot()
          if (loadCounter == 2) {
            throw new AccessDeniedException("")
          }
          result
        }

        override def refreshCredential() {
          // This should be called once
          retryCounter += 1
        }
      }
      testHadoopTable.open()
      testHadoopTable.setCacheManager(new NoCacheManager)
      testHadoopTable.refresh()
      assert(retryCounter == 1)
    })
  }

  test("retry credential exception to exceed max attempts") {
    withTempDir(dir => {
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING)
      createNonEmptyTable(engine, dir.getAbsolutePath, schema)

      var retryCounter = 0
      var loadCounter = 0
      // Release the lock after write a new version to the table
      val testHadoopTable = new HadoopTableForTest(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        List.empty[String].asJava) {

        override protected def loadLatestSnapshot(): Snapshot = {
          loadCounter += 1
          val result = super.loadLatestSnapshot()
          if (loadCounter >= 2) {
            throw new AccessDeniedException("")
          }
          result
        }

        override def refreshCredential() {
          // This should be called once
          retryCounter += 1
        }
      }
      testHadoopTable.open()
      // Disable cache for retry to work
      testHadoopTable.setCacheManager(new NoCacheManager)

      val e = intercept[Exception] {
        testHadoopTable.refresh()
      }
      assert(ExceptionUtils.recursiveCheck(_.isInstanceOf[AccessDeniedException]).test(e))
      assert(retryCounter == 3)
    })
  }

  test("write result has proper stats") {
    withTempDir(dir => {
      val schema = new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING)

      val table = new HadoopTableForTest(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        Seq.empty[String].asJava)
      table.open()

      val numColumns = 2
      val columnVectors = new Array[ColumnVector](numColumns)

      val dataBuffer = java.util.List.of(
        java.util.List.of(1, "Jack"),
        java.util.List.of(2, "Amy"))

      for (colIdx <- 0 until numColumns) {
        val colDataType = schema.at(colIdx).getDataType
        columnVectors(colIdx) = new DataColumnVectorView(dataBuffer, colIdx, colDataType)
      }

      val data = Utils.singletonCloseableIterator(
        new FilteredColumnarBatch(
          new DefaultColumnarBatch(dataBuffer.size, schema, columnVectors),
          Optional.empty))

      val result = table.writeParquet("", data, Map.empty[String, Literal].asJava)

      result.toInMemoryList.asScala
        .map(r => new AddFile(r.getStruct(SingleAction.ADD_FILE_ORDINAL)))
        .foreach(file => {
          assert(!file.getStatsJson.isEmpty)
          assert(file.getStatsJson == Optional.of(
            """
              |{"numRecords":2,"minValues":{"id":1,"name":"Amy"},
              |"maxValues":{"id":2,"name":"Jack"},"nullCount":{"id":0,"name":0}}
              |""".stripMargin.replaceAll("\n+", "")))
        })
    })
  }
}
