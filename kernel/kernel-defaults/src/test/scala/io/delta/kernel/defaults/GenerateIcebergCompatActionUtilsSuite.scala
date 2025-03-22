package io.delta.kernel.defaults

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.Table
import io.delta.kernel.internal.actions.GenerateIcebergCompatActionUtils
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable
import io.delta.kernel.utils.DataFileStatus

import org.apache.spark.sql.delta.DeltaLog

class GenerateIcebergCompatActionUtilsSuite extends DeltaTableWriteSuiteBase {

  // Testing this with real data right now is really hard because we don't actually fully support
  // column mapping writes (i.e. transformLogicalData doesn't support id mode) and need a way to
  // write column-mapping-id-based data

  test("E2E test using these APIs") {
    withTempDirAndEngine { (tempDir, engine) =>
      // val tempDir = "/tmp/test-this-for-me"

      // Create empty table with icebergWriterCompatV1 enabled (version 0)
      createEmptyTable(
        engine,
        tempDir,
        testSchema,
        tableProperties = Map("delta.enableIcebergWriterCompatV1" -> "true"))

      // Append data using generateIcebergCompatWriterV1AddAction (version 1)
      val txn1 = createWriteTxnBuilder(Table.forPath(engine, tempDir))
        .withMaxRetries(0)
        .build(engine)
      val txn1State = txn1.getTransactionState(engine)
      val actionsToCommit1 = Seq(
        new DataFileStatus(
          tempDir + "/foo1.parquet", // fake parquet file
          10,
          1000,
          Optional.empty()),
        new DataFileStatus(
          tempDir + "/foo2.parquet", // fake parquet file
          10,
          1000,
          Optional.empty())).map { dfs =>
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          txn1State,
          dfs,
          Collections.emptyMap(),
          true)
      }
      txn1.commit(engine, inMemoryIterable(toCloseableIterator(actionsToCommit1.iterator.asJava)))

      // Remove data using generateIcebergCompatWriterV1RemoveAction
      val txn2 = createWriteTxnBuilder(Table.forPath(engine, tempDir))
        .withMaxRetries(0)
        .build(engine)
      val txn2State = txn2.getTransactionState(engine)
      val actionsToCommit2 = Seq(
        new DataFileStatus(
          tempDir + "/foo1.parquet", // fake parquet file
          10,
          1000,
          Optional.empty())).map { dfs =>
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
          txn2State,
          dfs,
          Collections.emptyMap(),
          true)
      }
      txn2.commit(engine, inMemoryIterable(toCloseableIterator(actionsToCommit2.iterator.asJava)))

      // Rearrange data using generateIcebergCompatWriterV1AddAction and
      // generateIcebergCompatWriterV1RemoveAction
      val txn3 = createWriteTxnBuilder(Table.forPath(engine, tempDir))
        .withMaxRetries(0)
        .build(engine)
      val txn3State = txn3.getTransactionState(engine)
      val actionsToCommit3 = Seq(
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
          txn3State,
          new DataFileStatus(
            tempDir + "/foo2.parquet", // fake parquet file
            10,
            1000,
            Optional.empty()),
          Collections.emptyMap(),
          false),
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          txn3State,
          new DataFileStatus(
            tempDir + "/foo3.parquet", // fake parquet file
            10,
            1000,
            Optional.empty()),
          Collections.emptyMap(),
          false))
      txn3.commit(engine, inMemoryIterable(toCloseableIterator(actionsToCommit3.iterator.asJava)))

      // Below suffices to validate that the created actions can be parsed and some very basic
      // validation
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      DeltaLog.forTable(spark, tempDir).getSnapshotAt(3).allFiles.show() // forces log replay
      assert(deltaLog.getSnapshotAt(0).allFiles.count() == 0)
      assert(deltaLog.getSnapshotAt(1).allFiles.count() == 2) // add 2 files
      assert(deltaLog.getSnapshotAt(2).allFiles.count() == 1) // remove 1 file
      assert(deltaLog.getSnapshotAt(3).allFiles.count() == 1) // replace 1 file
      assert(deltaLog.getSnapshotAt(3).allFiles.collectAsList().asScala
        .map(_.path).headOption.exists(_.endsWith("foo3.parquet")))

      // Not sure why but allFiles.show() has dataChange = false; checked manually in JSON that
      // it was valid. Also tombstones are dropped as well in the snapshot.tombstones
      // I think as part of InMemoryLogReplay we do add.copy(dataChange=false)
      /*
      // Validate the writes with Spark
      val deltaLog = DeltaLog.forTable(spark, tempDir)

      val version0 = deltaLog.getSnapshotAt(0)
      assert(version0.allFiles.isEmpty)
      assert(version0.tombstones.isEmpty)

      // scalastyle:off
      val version1 = deltaLog.getSnapshotAt(1)
      println("Version 1 allFiles")
      version1.allFiles
        .select("path", "size", "modificationTime", "dataChange").show(truncate = false)
      assert(version1.tombstones.isEmpty)

      val version2 = deltaLog.getSnapshotAt(2)
      println("Version 2 allFiles")
      version2.allFiles
        .select("path", "size", "modificationTime", "dataChange").show(truncate = false)
      println("Version 2 tombstones")
      version2.tombstones
        .select("path", "size", "dataChange").show(truncate = false)

      val version3 = deltaLog.getSnapshotAt(3)
      println("Version 3 allFiles")
      version3.allFiles
        .select("path", "size", "modificationTime", "dataChange").show(truncate = false)
      println("Version 3 tombstones")
      version3.tombstones
        .select("path", "size", "dataChange").show(truncate = false)

       */
    }
  }
}
