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
package io.delta.kernel.defaults

// import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.Table
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.hook.LogCompactionHook

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class LogCompactionSuite extends AnyFunSuite with DeltaTableWriteSuiteBase with TestUtils {

  test("Compaction containing different action types") {
    withTempDirAndEngine { (tblPath, engine) =>
      // commit 0 - add data
      appendData(
        engine,
        tblPath,
        isNewTable = true,
        testSchema,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))

      // commit 1 - set a metadata prop
      val newTblProps = Map(TableConfig.CHECKPOINT_POLICY.getKey -> "v2")
      updateTableMetadata(engine, tblPath, tableProperties = newTblProps)

      // commit 2 - add domain metadata
      val dmTxn = createTxn(engine, tblPath, withDomainMetadataSupported = true)
      dmTxn.addDomainMetadata("testDomain", "testConfig")
      commitAppendData(engine, dmTxn, Seq.empty)

      // commit 3 - add more data
      appendData(
        engine,
        tblPath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2))

      // create the compaction file(s)
      val dataPath = new Path(s"file:${tblPath}")
      val logPath = new Path(s"file:${tblPath}", "_delta_log")
      val hook = new LogCompactionHook(dataPath, logPath, 0, 3, 0)
      hook.threadSafeInvoke(engine)

      val hadoopFileIO = new HadoopFileIO(new Configuration())
      val metricEngine = new MetricsEngine(hadoopFileIO)
      val table = Table.forPath(metricEngine, tblPath)

      val snapshot = table.getLatestSnapshot(metricEngine).asInstanceOf[SnapshotImpl]
      val checkpointProp =
        snapshot.getMetadata().getConfiguration.get(TableConfig.CHECKPOINT_POLICY.getKey)
      assert(checkpointProp == "v2")

      // this is the read that the snapshot did
      val propCompactionsRead = metricEngine.getJsonHandler.getCompactionsRead
      assert(propCompactionsRead.toSet === Set((0, 3)))

      metricEngine.resetMetrics()
      val domainMetadata = snapshot.getDomainMetadata("testDomain")
      assert(domainMetadata.isPresent())
      assert(domainMetadata.get == "testConfig")

      // getting domain metadata requires another log-reply, so check that this one also used the
      // compaction
      val dmCompactionsRead = metricEngine.getJsonHandler.getCompactionsRead
      assert(dmCompactionsRead.toSet === Set((0, 3)))

      // ensure the data is all there
      metricEngine.resetMetrics()
      val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++ dataBatches2.flatMap(_.toTestRows)
      checkTable(tblPath, expectedAnswer, engine = metricEngine)

      val readCompactionsRead = metricEngine.getJsonHandler.getCompactionsRead
      assert(readCompactionsRead.toSet === Set((0, 3)))
    }
  }

  def testWithCompactions(
      versionsToWrite: Seq[Int], // highest version MUST be last!
      versionToRead: Option[Long],
      doRemoves: Boolean,
      compactions: Seq[(Int, Int)],
      expectedDeltasToBeRead: Set[Int],
      expectedCompactionsToBeRead: Set[(Int, Int)]) {
    withTempDir { tmpDir =>
      val tablePath = tmpDir.getCanonicalPath
      val hadoopFileIO = new HadoopFileIO(new Configuration() {
        {
          // Set the batch sizes to small so that we get to test the multiple batch scenarios.
          set("delta.kernel.default.parquet.reader.batch-size", "2");
          set("delta.kernel.default.json.reader.batch-size", "2");
        }
      })
      val engine = new MetricsEngine(hadoopFileIO)
      var expectedRows: Set[Long] = Set()
      versionsToWrite.foreach { i =>
        // if we're removing, then on odd commits, remove the lower 10 of the previous 20 rows added
        if (doRemoves && i % 2 == 1) {
          val prev = i - 1;
          val low = prev * 10
          val high = prev * 10 + 10
          val deleteQuery = "DELETE FROM delta.`%s` WHERE id >= %d AND id < %d".format(
            tablePath,
            low,
            high)
          spark.sql(deleteQuery)
          if (versionToRead.isEmpty || versionToRead.get >= i) {
            expectedRows --= (low until high).map(i => i.toLong)
          }
          // if (i == compactions(0).1) {
          //   // ensure we put a DM in a compaction

          // }
        } else {
          val low = i * 10
          // if we're removing, add 20 rows as the first 10 will be removed by the next version,
          // otherwise add 10 rows
          val high = if (doRemoves) low + 20 else low + 10
          spark.range(low, high).write
            .format("delta")
            .mode("append")
            .save(tablePath)

          if (versionToRead.isEmpty || versionToRead.get >= i) {
            expectedRows ++= (low until high).map(i => i.toLong)
          }
        }
      }

      val dataPath = new Path(s"file:${tablePath}")
      val logPath = new Path(s"file:${tablePath}", "_delta_log")
      // create the compaction file(s)
      compactions.foreach { compaction =>
        val hook = new LogCompactionHook(
          dataPath,
          logPath,
          compaction._1,
          compaction._2,
          0)
        hook.threadSafeInvoke(engine)
      }
      engine.resetMetrics()

      checkTable(
        path = tablePath,
        expectedAnswer = expectedRows.toSeq.map(i => TestRow(i)),
        engine = engine,
        version = versionToRead)

      val actualJsonVersionsRead = engine.getJsonHandler.getVersionsRead
      val actualCompactionsRead = engine.getJsonHandler.getCompactionsRead
      assert(actualJsonVersionsRead.toSet == expectedDeltasToBeRead)
      assert(actualCompactionsRead.toSet == expectedCompactionsToBeRead)
    }
  }

  Seq(Seq((0, 3)), Seq((3, 5)), Seq((5, 9)), Seq((0, 3), (5, 8))).foreach {
    compactions =>
      Seq(true, false).foreach { doRemoves =>
        val compactionStr = compactions.mkString(", ")
        test(s"Compaction(s) at $compactionStr (no checkpoint, removes: $doRemoves)") {
          // for these tests, write 0 - 9 (inclusive)
          val versionsToWrite = (0 to 9)
          var expectedDeltasToBeRead = versionsToWrite.toSet
          compactions.foreach { compaction =>
            // subtract out the compaction versions from the full set
            expectedDeltasToBeRead &~= (compaction._1 to compaction._2).toSet
          }
          testWithCompactions(
            versionsToWrite,
            versionToRead = None,
            doRemoves,
            compactions,
            expectedDeltasToBeRead,
            compactions.toSet)
        }
      }
  }

  Seq(Seq((3, 5)), Seq((8, 11)), Seq((8, 12), (11, 15)), Seq((11, 13), (15, 17))).foreach {
    compactions =>
      Seq(true, false).foreach { doRemoves =>
        val compactionStr = compactions.mkString(", ")
        test(s"Compaction(s) at $compactionStr (with checkpoint, removes: $doRemoves)") {
          // for these tests, write 0 - 19 (inclusive), will checkpoint at 10
          val versionsToWrite = (0 to 19)
          val versionsAfterCheckpoint = (11 to 19)
          var expectedDeltasToBeRead = versionsAfterCheckpoint.toSet
          var expectedCompactionsToBeRead = Set[(Int, Int)]()
          compactions.foreach { compaction =>
            if (compaction._1 > 10) { // only use if after checkpoint
              // subtract out the compaction versions from the full set
              expectedDeltasToBeRead &~= (compaction._1 to compaction._2).toSet
              // add to expected compactions
              expectedCompactionsToBeRead += compaction
            }
          }
          testWithCompactions(
            versionsToWrite,
            versionToRead = None,
            doRemoves,
            compactions,
            expectedDeltasToBeRead,
            expectedCompactionsToBeRead)
        }
      }
  }

  test("Compaction with overlap") {
    testWithCompactions(
      versionsToWrite = (0 to 9),
      versionToRead = None,
      doRemoves = true,
      compactions = Seq((0, 3), (2, 4)),
      expectedDeltasToBeRead = Set(0, 1, 5, 6, 7, 8, 9),
      expectedCompactionsToBeRead = Set((2, 4)))
  }

  test("Compaction is whole range") {
    testWithCompactions(
      versionsToWrite = (0 to 5),
      versionToRead = None,
      doRemoves = true,
      compactions = Seq((0, 5)),
      expectedDeltasToBeRead = Set(),
      expectedCompactionsToBeRead = Set((0, 5)))
  }

  test("Compaction out of range") {
    testWithCompactions(
      versionsToWrite = (0 to 9),
      versionToRead = Some(6),
      doRemoves = true,
      compactions = Seq((1, 3), (5, 8)),
      expectedDeltasToBeRead = Set(0, 4, 5, 6),
      expectedCompactionsToBeRead = Set((1, 3)))
  }
}
