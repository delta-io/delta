package io.delta.storage

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.{DeltaLog, LogStoreSuiteBase}
import org.apache.spark.sql.delta.storage.LogStoreAdaptor
import org.apache.spark.sql.delta.test.DeltaTestImplicits.OptimisticTxnTestHelper
import org.apache.spark.util.Utils

abstract class ContributesLogStoreSuiteBase extends LogStoreSuiteBase {

  protected override def testInitFromSparkConf(): Unit = {
    test("instantiation through SparkConf") {
      assert(spark.sparkContext.getConf.get(logStoreClassConfKey) == logStoreClassName)
      assert(org.apache.spark.sql.delta.storage.LogStore(spark).asInstanceOf[LogStoreAdaptor]
        .logStoreImpl.getClass.getName == logStoreClassName)
    }
  }

//  protected def testSimpleLogStore(): Unit = {
//    test("simple log store test") {
//      val tempDir = Utils.createTempDir()
//      val log1 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
//      assert(log1.store.asInstanceOf[LogStoreAdaptor]
//        .logStoreImpl.getClass.getName == logStoreClassName)
//
//      val txn = log1.startTransaction()
//      val file = AddFile("1", Map.empty, 1, 1, true) :: Nil
//      txn.commitManually(file: _*)
//      log1.checkpoint()
//
//      DeltaLog.clearCache()
//      val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
//      assert(log2.store.asInstanceOf[LogStoreAdaptor]
//        .logStoreImpl.getClass.getName == logStoreClassName)
//
//      assert(log2.readLastCheckpointFile().map(_.version) === Some(0L))
//      assert(log2.snapshot.allFiles.count == 1)
//    }
//  }
}
