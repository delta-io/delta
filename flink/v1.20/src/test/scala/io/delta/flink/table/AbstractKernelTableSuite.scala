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

import dev.failsafe.FailsafeException
import io.delta.flink.TestHelper
import io.delta.kernel.Snapshot
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{AddFile, SingleAction}
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.{CloseableIterable, DataFileStatus}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI
import java.nio.file.AccessDeniedException
import java.util.{ConcurrentModificationException, Optional, UUID}
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

class AbstractKernelTableSuite extends AnyFunSuite with TestHelper {

  test("normalize URI") {
    assert(AbstractKernelTable.normalize(URI.create("file:/var")).toString == "file:///var/")
    assert(AbstractKernelTable.normalize(URI.create("file:///var")).toString == "file:///var/")
    assert(AbstractKernelTable.normalize(URI.create("s3://host/var")).toString == "s3://host/var/")
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

        override def onSnapshotReloaded() {
          // This should be called once
          retryCounter += 1
        }
      }

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

        override def onCredentialsRefreshed() {
          // This should be called once
          retryCounter += 1
        }
      }
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

        override def onCredentialsRefreshed() {
          // This should be called once
          retryCounter += 1
        }
      }
      val e = intercept[FailsafeException] {
        testHadoopTable.refresh()
      }
      assert(e.getCause.isInstanceOf[AccessDeniedException])
      assert(retryCounter == 3)
    })
  }

  test("force cancel ongoing log replay") {
    withTempDir { dir =>
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING)
      createNonEmptyTable(engine, dir.getAbsolutePath, schema)

      var callcounter = 0

      val table = new HadoopTableForTest(
        dir.toURI, Map.empty[String, String].asJava, schema, Seq.empty[String].asJava) {

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
}
