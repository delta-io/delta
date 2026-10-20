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

package org.apache.spark.sql.delta.optimize

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._

import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaColumnMapping, DeltaIOException, DeltaLog, DeltaTableProvider, DeltaUnsupportedOperationException}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.DeltaPurgeOperation
import org.apache.spark.sql.delta.commands.VacuumCommand.generateCandidateFileMap
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.util.DeltaFileOperations
import io.delta.tables.DeltaTable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.logging.log4j.Level
import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.hadoop.Footer

import org.apache.spark.{BarrierTaskContext, SparkEnv, SparkException}
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.SerializableConfiguration

class DeltaReorgSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils
  with DeltaTableProvider
  with DeletionVectorsTestUtils {

  import testImplicits._

  def executePurge(table: String, condition: Option[String] = None): Unit = {
    condition match {
      case Some(cond) => sql(s"REORG TABLE delta.`$table` WHERE $cond APPLY (PURGE)")
      case None => sql(s"REORG TABLE delta.`$table` APPLY (PURGE)")
    }
  }

  test("Purge DVs will combine small files") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      sql(s"DELETE FROM delta.`$path` WHERE id IN (0, 99)")
      assert(log.update().allFiles.filter(_.deletionVector != null).count() === 2)
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> "1073741824") { // 1gb
        executePurge(path)
      }
      val (addFiles, _) = getFileActionsInLastVersion(log)
      assert(addFiles.size === 1, "files should be combined")
      assert(addFiles.forall(_.deletionVector === null))
      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        (1 to 98).toDF())

      // Verify commit history and operation metrics
      checkOpHistory(
        tablePath = path,
        expOpParams = Map("applyPurge" -> "true", "predicate" -> "[]"),
        numFilesRemoved = 2,
        numFilesAdded = 1)
    }
  }

  test("Purge DVs") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      sql(s"DELETE FROM delta.`$path` WHERE id IN (0, 99)")
      assert(log.update().allFiles.filter(_.deletionVector != null).count() === 2)

      // First purge
      executePurge(path)
      val (addFiles, _) = getFileActionsInLastVersion(log)
      assert(addFiles.size === 1) // two files are combined
      assert(addFiles.forall(_.deletionVector === null))
      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        (1 to 98).toDF())

      // Verify commit history and operation metrics
      checkOpHistory(
        tablePath = path,
        expOpParams = Map("applyPurge" -> "true", "predicate" -> "[]"),
        numFilesRemoved = 2,
        numFilesAdded = 1)

      // Second purge is a noop
      val versionBefore = log.update().version
      executePurge(path)
      val versionAfter = log.update().version
      assert(versionBefore === versionAfter)
    }
  }

  test("Purge DVs when tasks cannot see a SparkSession") {
    // On a cluster, executors have no active or default SparkSession. In local mode the tasks
    // fall back to the JVM-wide default session, so clear it to reproduce executor behavior.
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString
      sql(s"DELETE FROM delta.`$path` WHERE id IN (0, 99)")

      val defaultSession = SparkSession.getDefaultSession
      SparkSession.clearDefaultSession()
      try {
        executePurge(path)
      } finally {
        defaultSession.foreach(SparkSession.setDefaultSession)
      }
      val (addFiles, _) = getFileActionsInLastVersion(log)
      assert(addFiles.nonEmpty)
      assert(addFiles.forall(_.deletionVector === null))
      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        (1 to 98).toDF())
    }
  }

  test("Purge a non-DV table is a noop") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF()
    withTempDeltaTable(targetDf, enableDVs = false) { (_, log) =>
      val versionBefore = log.update().version
      executePurge(log.dataPath.toString)
      val versionAfter = log.update().version
      assert(versionBefore === versionAfter)
    }
  }

  test("Purge some partitions of a table with DV") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 1)
      .withColumn("part", col("id") % 4)
      .toDF()
    withTempDeltaTable(targetDf, partitionBy = Seq("part")) { (_, log) =>
      val path = log.dataPath
      // Delete one row from each partition
      sql(s"DELETE FROM delta.`$path` WHERE id IN (48, 49, 50, 51)")
      val (addFiles1, _) = getFileActionsInLastVersion(log)
      assert(addFiles1.size === 4)
      assert(addFiles1.forall(_.deletionVector !== null))
      // PURGE two partitions
      sql(s"REORG TABLE delta.`$path` WHERE part IN (0, 2) APPLY (PURGE)")
      val (addFiles2, _) = getFileActionsInLastVersion(log)
      assert(addFiles2.size === 2)
      assert(addFiles2.forall(_.deletionVector === null))

      // Verify commit history and operation metrics
      checkOpHistory(
        tablePath = path.toString,
        expOpParams = Map("applyPurge" -> "true", "predicate" -> "[\"'part IN (0,2)\"]"),
        numFilesRemoved = 2,
        numFilesAdded = 2)
    }
  }

  private def checkOpHistory(
      tablePath: String,
      expOpParams: Map[String, String],
      numFilesRemoved: Long,
      numFilesAdded: Long): Unit = {
    val (opName, opParams, opMetrics) = DeltaTable.forPath(tablePath)
      .history(1)
      .select("operation", "operationParameters", "operationMetrics")
      .as[(String, Map[String, String], Map[String, String])]
      .head()
    assert(opName === "REORG")
    assert(opParams === expOpParams)
    assert(opMetrics("numAddedFiles").toLong === numFilesAdded)
    assert(opMetrics("numRemovedFiles").toLong === numFilesRemoved)
    // Because each deleted file has a DV associated it which gets rewritten as part of PURGE
    assert(opMetrics("numDeletionVectorsRemoved").toLong === numFilesRemoved)
  }

  /**
   * Get all parquet footers for the input `files`, used only for testing.
   *
   * @param files the sequence of `AddFile` used to read the parquet footers
   *              by the data file path in each `AddFile`.
   * @param log the delta log used to get the configuration and data path.
   * @return the sequence of the corresponding parquet footers, corresponds to
   *         the sequence of `AddFile`.
   */
  private def getParquetFooters(
      files: Seq[AddFile],
      log: DeltaLog): Seq[Footer] = {
    val serializedConf = new SerializableConfiguration(log.newDeltaHadoopConf())
    val dataPath = new Path(log.dataPath.toString)
    val nameToAddFileMap = generateCandidateFileMap(dataPath, files)
    val fileStatuses = nameToAddFileMap.map { case (absPath, addFile) =>
      new FileStatus(
        /* length */ addFile.size,
        /* isDir */ false,
        /* blockReplication */ 0,
        /* blockSize */ 1,
        /* modificationTime */ addFile.modificationTime,
        new Path(absPath)
      )
    }
    DeltaFileOperations.readParquetFootersInParallel(
      serializedConf.value,
      fileStatuses.toList,
      ignoreCorruptFiles = false
    )
  }

  test("Purge dropped columns of a table without DV") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5)
      .withColumn("id_dropped", col("id") % 4)
      .toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      val (addFiles1, _) = getFileActionsInLastVersion(log)
      assert(addFiles1.size === 5)
      val footers1 = getParquetFooters(addFiles1, log)
      footers1.foreach { footer =>
        val fields = footer.getParquetMetadata.getFileMetaData.getSchema.getFields
        assert(fields.size == 2)
        assert(fields.toArray.map { _.toString }.contains("optional int64 id_dropped"))
      }

      // enable column-mapping first
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | SET TBLPROPERTIES (
           |   'delta.columnMapping.mode' = 'name'
           | )
           |""".stripMargin
      )
      // drop the extra column by alter table and run REORG PURGE
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | DROP COLUMN id_dropped
           |""".stripMargin
      )
      executePurge(path)

      val (addFiles2, _) = getFileActionsInLastVersion(log)
      assert(addFiles2.size === 1)
      val footers2 = getParquetFooters(addFiles2, log)
      footers2.foreach { footer =>
        val fields = footer.getParquetMetadata.getFileMetaData.getSchema.getFields
        assert(fields.size == 1)
        assert(!fields.toArray.map { _.toString }.contains("optional int64 id_dropped"))
      }
    }
  }

  test("REORG PURGE footer scan honors memory-admission gate, batching and parallelism") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5)
      .withColumn("id_dropped", col("id") % 4)
      .toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString
      sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      sql(s"ALTER TABLE delta.`$path` DROP COLUMN id_dropped")
      // Files appended after the drop never had `id_dropped`, so only the original files qualify.
      spark.range(100, 160, 1, numPartitions = 3).toDF()
        .write.format(writeFormat).mode("append").save(path)

      val snapshot = log.update()
      val files = snapshot.allFiles.collect().toSeq
      val purge = new DeltaPurgeOperation()

      def select(
          batchSize: Int,
          parallelism: Int,
          memoryAdmissionEnabled: Boolean = false): Set[String] = {
        withSQLConf(
            DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_BATCH_SIZE.key -> batchSize.toString,
            DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_PARALLELISM.key -> parallelism.toString,
            DeltaSQLConf.DELTA_REORG_FOOTER_MEMORY_ADMISSION_ENABLED.key ->
              memoryAdmissionEnabled.toString,
            "spark.sql.leafNodeDefaultParallelism" -> "1") {
          purge.filterFilesToReorg(spark, snapshot, files).map(_.path).toSet
        }
      }

      val expected = select(files.size + 5, parallelism = 1)
      assert(expected.nonEmpty && expected.size < files.size,
        s"PURGE should select a strict, non-empty subset of ${files.size} files")
      Seq(0, 1, 2, files.size - 1).foreach { batchSize =>
        Seq(1, 3).foreach { parallelism =>
          assert(select(batchSize, parallelism) === expected,
            s"batchSize=$batchSize parallelism=$parallelism changed selection")
        }
      }
      Seq(0, 2).foreach { batchSize =>
        assert(select(batchSize, parallelism = 3, memoryAdmissionEnabled = true) === expected,
          s"memory admission changed selection for batchSize=$batchSize")
      }

      val footerLogs = new LogAppender("REORG footer scan parallelism", maxEvents = 1000)
      footerLogs.setThreshold(Level.INFO)
      val footerLogger = DeltaFileOperations.getClass.getName.stripSuffix("$")
      withLogAppender(
          footerLogs,
          loggerNames = Seq(footerLogger),
          level = Some(Level.INFO)) {
        select(batchSize = 3, parallelism = 3, memoryAdmissionEnabled = true)
        select(batchSize = 0, parallelism = 7, memoryAdmissionEnabled = true)
        select(batchSize = 3, parallelism = 13, memoryAdmissionEnabled = false)
      }
      val messages = footerLogs.loggingEvents.map(_.getMessage.getFormattedMessage)
      val details = messages.mkString(" | ")
      val readLog = """Filtering (\d+) Parquet footers with parallelism (\d+)""".r
      val observedBatchSizes = messages.collect {
        case readLog(size, "3") => size.toInt
      }.sorted
      val expectedBatchSizes = files.grouped(3).map(_.size).toSeq.sorted
      assert(observedBatchSizes === expectedBatchSizes,
        s"footer reader observed batches $observedBatchSizes instead of $expectedBatchSizes " +
          s"at parallelism=3 in $details")
      val observedUnbatchedSizes = messages.collect {
        case readLog(size, "7") => size.toInt
      }
      assert(observedUnbatchedSizes === Seq(files.size),
        s"footer reader observed unbatched calls $observedUnbatchedSizes instead of " +
          s"${Seq(files.size)} at parallelism=7 in $details")
      val legacyReadLog = """Reading (\d+) Parquet footers with parallelism (\d+)""".r
      val observedLegacyBatchSizes = messages.collect {
        case legacyReadLog(size, "13") => size.toInt
      }.sorted
      assert(observedLegacyBatchSizes === expectedBatchSizes,
        s"legacy footer reader observed batches $observedLegacyBatchSizes instead of " +
          s"$expectedBatchSizes at parallelism=13 in $details")
      assert(!messages.exists {
        case readLog(_, "13") => true
        case _ => false
      }, s"memory-admission reader unexpectedly ran with disabled gate in $details")
    }
  }

  test("legacy and admission footer scans propagate fatal errors without hanging") {
    def statusThrowing(fatal: OutOfMemoryError): FileStatus = new FileStatus() {
      override def getPath: Path = throw fatal
    }

    val admission = new DeltaFileOperations.ParquetFooterMemoryAdmission(
      memoryLimitBytes = 1024,
      activeReaderLimit = 1,
      decodedObjectAllowanceBytes = 0)
    // scalastyle:off sparkThreadPools
    val executor = Executors.newSingleThreadExecutor()
    // scalastyle:on sparkThreadPools

    def captureWithinTimeout(block: => Unit): Throwable = {
      val result = executor.submit(new Callable[Throwable] {
        override def call(): Throwable = {
          try {
            block
            null
          } catch {
            case t: Throwable => t
          }
        }
      })
      try result.get(10, TimeUnit.SECONDS) finally result.cancel(true)
    }

    try {
      val legacyFatal = new OutOfMemoryError("legacy fatal error")
      val legacyResult = captureWithinTimeout {
        DeltaFileOperations.readParquetFootersInParallel(
          new Configuration(false),
          Seq(statusThrowing(legacyFatal)),
          ignoreCorruptFiles = false,
          parallelism = 1)
      }
      assert(legacyResult eq legacyFatal)

      val admissionFatal = new OutOfMemoryError("admission fatal error")
      val admissionResult = captureWithinTimeout {
        DeltaFileOperations.filterParquetFilesInParallel(
          new Configuration(false),
          Seq(() -> statusThrowing(admissionFatal)),
          ignoreCorruptFiles = false,
          parallelism = 1,
          admission = admission) { (_, _) => true }
      }
      assert(admissionResult eq admissionFatal)
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    }
  }

  test("REORG footer scan and admission defaults") {
    val admission = DeltaFileOperations.ParquetFooterMemoryAdmission
    assert(DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_BATCH_SIZE.defaultValue.contains(500))
    assert(DeltaSQLConf.DELTA_REORG_FOOTER_MEMORY_ADMISSION_ENABLED.defaultValue.contains(false))
    assert(DeltaSQLConf.DELTA_REORG_FOOTER_MEMORY_ADMISSION_MAX_ACTIVE_READERS
      .defaultValue.contains(64))
    assert(DeltaSQLConf.DELTA_REORG_FOOTER_MEMORY_ADMISSION_EXECUTOR_MEMORY_FRACTION
      .defaultValue.contains(0.10))
    assert(admission.DECODED_OBJECT_ALLOWANCE_BYTES === 512 * 1024)
    assert(admission.MEMORY_PERMIT_BYTES === 1024L)
    assert(admission.executorMemoryLimit(
      20L * 1024 * 1024, executorMemoryFraction = 0.10) === 2L * 1024 * 1024)
    assert(admission.executorMemoryLimit(
      20L * 1024 * 1024, executorMemoryFraction = 0.25) === 5L * 1024 * 1024)
    assert(admission.executorMemoryLimit(
      10L * 1024 * 1024 * 1024,
      executorMemoryFraction = 0.10) === 1024L * 1024 * 1024)
  }

  test("REORG footer admission is shared by Spark tasks in one executor") {
    val results = spark.sparkContext.parallelize(Seq(0, 1), numSlices = 2)
      .barrier()
      .mapPartitions { _ =>
        val admission = DeltaFileOperations.ParquetFooterMemoryAdmission.global
        admission.withReader("barrier reader") { _ =>
          val barrierContext = BarrierTaskContext.get()
          barrierContext.barrier()
          val result = (
            SparkEnv.get.executorId,
            System.identityHashCode(admission),
            admission.availableReaderPermits,
            admission.activeReaderLimit)
          barrierContext.barrier()
          Iterator.single(result)
        }
      }
      .collect()

    assert(results.length === 2)
    assert(results.map(_._1).distinct.length === 1)
    assert(results.map(_._2).distinct.length === 1)
    results.foreach { case (_, _, availableReaderPermits, activeReaderLimit) =>
      assert(availableReaderPermits === activeReaderLimit - 2)
    }
  }

  test("REORG footer admission is executor-wide, weighted, and cleans up reservations") {
    val admission = new DeltaFileOperations.ParquetFooterMemoryAdmission(
      memoryLimitBytes = 30L * 1024,
      activeReaderLimit = 3,
      decodedObjectAllowanceBytes = 0)
    val largeAllocated = new CountDownLatch(1)
    val releaseLarge = new CountDownLatch(1)
    val smallAllocated = new CountDownLatch(1)
    val releaseSmall = new CountDownLatch(1)
    val tinyAllocated = new CountDownLatch(1)
    val releaseTiny = new CountDownLatch(1)
    val oversizedAllocated = new CountDownLatch(1)
    val releaseOversized = new CountDownLatch(1)
    val afterOversizedAllocated = new CountDownLatch(1)
    val releaseAfterOversized = new CountDownLatch(1)

    // scalastyle:off sparkThreadPools
    val executor = Executors.newFixedThreadPool(3)
    // scalastyle:on sparkThreadPools

    def hold(
        name: String,
        size: Int,
        allocated: CountDownLatch,
        release: CountDownLatch): Callable[Unit] = new Callable[Unit] {
      override def call(): Unit = admission.withReader(name) { allocator =>
        allocator.allocate(size)
        allocated.countDown()
        release.await()
      }
    }

    val large = executor.submit(hold("large", 20 * 1024, largeAllocated, releaseLarge))
    val small = executor.submit(hold("small", 10 * 1024, smallAllocated, releaseSmall))
    var tiny: java.util.concurrent.Future[Unit] = null
    var oversized: java.util.concurrent.Future[Unit] = null
    var afterOversized: java.util.concurrent.Future[Unit] = null
    try {
      assert(largeAllocated.await(10, TimeUnit.SECONDS))
      assert(smallAllocated.await(10, TimeUnit.SECONDS))
      assert(admission.availableMemoryBytes === 0)
      assert(admission.availableReaderPermits === 1)

      tiny = executor.submit(hold("tiny", 1024, tinyAllocated, releaseTiny))
      eventually(timeout(10.seconds)) {
        assert(admission.numMemoryWaiters === 1)
      }
      assert(tinyAllocated.getCount === 1)

      releaseSmall.countDown()
      small.get(10, TimeUnit.SECONDS)
      assert(tinyAllocated.await(10, TimeUnit.SECONDS))
      assert(admission.availableMemoryBytes === 9L * 1024)

      releaseTiny.countDown()
      releaseLarge.countDown()
      tiny.get(10, TimeUnit.SECONDS)
      large.get(10, TimeUnit.SECONDS)
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)

      oversized = executor.submit(
        hold("oversized", 31 * 1024, oversizedAllocated, releaseOversized))
      assert(oversizedAllocated.await(10, TimeUnit.SECONDS))
      assert(admission.availableMemoryBytes === 0)

      afterOversized = executor.submit(
        hold("after oversized", 1024, afterOversizedAllocated, releaseAfterOversized))
      eventually(timeout(10.seconds)) {
        assert(admission.numMemoryWaiters === 1)
      }
      assert(afterOversizedAllocated.getCount === 1)

      releaseOversized.countDown()
      oversized.get(10, TimeUnit.SECONDS)
      assert(afterOversizedAllocated.await(10, TimeUnit.SECONDS))
      releaseAfterOversized.countDown()
      afterOversized.get(10, TimeUnit.SECONDS)
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)

      intercept[IllegalStateException] {
        admission.withReader("failed predicate") { allocator =>
          allocator.allocate(5 * 1024)
          throw new IllegalStateException("injected predicate failure")
        }
      }
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)

      val fatalFailure = new OutOfMemoryError("injected fatal failure")
      val propagatedFatal = intercept[OutOfMemoryError] {
        admission.withReader("fatal predicate") { allocator =>
          allocator.allocate(5 * 1024)
          throw fatalFailure
        }
      }
      assert(propagatedFatal eq fatalFailure)
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)
    } finally {
      releaseTiny.countDown()
      releaseSmall.countDown()
      releaseLarge.countDown()
      releaseOversized.countDown()
      releaseAfterOversized.countDown()
      if (tiny != null) tiny.cancel(true)
      if (oversized != null) oversized.cancel(true)
      if (afterOversized != null) afterOversized.cancel(true)
      large.cancel(true)
      small.cancel(true)
      executor.shutdownNow()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    }
  }

  test("REORG footer active-reader admission limits concurrency and is interruptible") {
    val admission = new DeltaFileOperations.ParquetFooterMemoryAdmission(
      memoryLimitBytes = 10L * 1024,
      activeReaderLimit = 1,
      decodedObjectAllowanceBytes = 0)
    val firstReaderStarted = new CountDownLatch(1)
    val releaseFirstReader = new CountDownLatch(1)
    val secondReaderStarted = new CountDownLatch(1)

    // scalastyle:off sparkThreadPools
    val executor = Executors.newFixedThreadPool(2)
    // scalastyle:on sparkThreadPools
    val firstReader = executor.submit(new Callable[Unit] {
      override def call(): Unit = admission.withReader("first reader") { _ =>
        firstReaderStarted.countDown()
        releaseFirstReader.await()
      }
    })
    var secondReader: java.util.concurrent.Future[Unit] = null
    try {
      assert(firstReaderStarted.await(10, TimeUnit.SECONDS))
      assert(admission.availableReaderPermits === 0)

      secondReader = executor.submit(new Callable[Unit] {
        override def call(): Unit = admission.withReader("second reader") { _ =>
          secondReaderStarted.countDown()
        }
      })
      eventually(timeout(10.seconds)) {
        assert(admission.numReaderWaiters === 1)
      }
      assert(secondReaderStarted.getCount === 1)

      assert(secondReader.cancel(true))
      eventually(timeout(10.seconds)) {
        assert(admission.numReaderWaiters === 0)
        assert(admission.availableReaderPermits === 0)
      }

      releaseFirstReader.countDown()
      firstReader.get(10, TimeUnit.SECONDS)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)

      admission.withReader("reader after interruption") { _ => () }
      assert(admission.availableReaderPermits === admission.activeReaderLimit)
    } finally {
      releaseFirstReader.countDown()
      if (secondReader != null) secondReader.cancel(true)
      firstReader.cancel(true)
      executor.shutdownNow()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    }
  }

  test("REORG footer admission wait is interruptible") {
    val admission = new DeltaFileOperations.ParquetFooterMemoryAdmission(
      memoryLimitBytes = 10L * 1024,
      activeReaderLimit = 2,
      decodedObjectAllowanceBytes = 0)
    val blockerAllocated = new CountDownLatch(1)
    val releaseBlocker = new CountDownLatch(1)

    // scalastyle:off sparkThreadPools
    val executor = Executors.newFixedThreadPool(2)
    // scalastyle:on sparkThreadPools
    val blocker = executor.submit(new Callable[Unit] {
      override def call(): Unit = admission.withReader("blocker") { allocator =>
        allocator.allocate(10 * 1024)
        blockerAllocated.countDown()
        releaseBlocker.await()
      }
    })
    var waiter: java.util.concurrent.Future[Unit] = null
    try {
      assert(blockerAllocated.await(10, TimeUnit.SECONDS))
      waiter = executor.submit(new Callable[Unit] {
        override def call(): Unit = admission.withReader("waiter") { allocator =>
          allocator.allocate(1024)
        }
      })
      eventually(timeout(10.seconds)) {
        assert(admission.numMemoryWaiters === 1)
      }
      assert(waiter.cancel(true))
      eventually(timeout(10.seconds)) {
        assert(admission.numMemoryWaiters === 0)
        assert(admission.availableReaderPermits === 1)
      }

      releaseBlocker.countDown()
      blocker.get(10, TimeUnit.SECONDS)
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)
    } finally {
      releaseBlocker.countDown()
      if (waiter != null) waiter.cancel(true)
      blocker.cancel(true)
      executor.shutdownNow()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    }
  }

  test("REORG footer admission errors are explicit and not treated as corrupt files") {
    withTempDir { tempDir =>
      val parquetDir = new File(tempDir, "valid")
      spark.range(1).coalesce(1).write.parquet(parquetDir.getCanonicalPath)
      val conf = spark.sessionState.newHadoopConf()
      val parquetPath = new Path(parquetDir.toURI)
      val status = parquetPath.getFileSystem(conf).listStatus(parquetPath)
        .find(_.getPath.getName.endsWith(".parquet")).get
      val admission = new DeltaFileOperations.ParquetFooterMemoryAdmission(
        memoryLimitBytes = 10L * 1024,
        activeReaderLimit = 1,
        decodedObjectAllowanceBytes = 0)

      val generatedError = intercept[
        DeltaFileOperations.ParquetFooterMemoryAdmissionException] {
        admission.withReader("multiple buffers") { allocator =>
          allocator.allocate(1024)
          allocator.allocate(1024)
        }
      }
      assert(generatedError.getMessage.contains("requested multiple buffers"))
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)

      def allocatorThrowing(error: RuntimeException): ByteBufferAllocator =
        new ByteBufferAllocator {
          override def allocate(size: Int): ByteBuffer = throw error
          override def release(buffer: ByteBuffer): Unit = {}
          override def isDirect: Boolean = false
        }

      Seq(true, false).foreach { ignoreCorruptFiles =>
        val admissionError =
          new DeltaFileOperations.ParquetFooterMemoryAdmissionException("injected admission error")
        val propagatedError = intercept[
          DeltaFileOperations.ParquetFooterMemoryAdmissionException] {
          DeltaFileOperations.readParquetFooter(
            conf,
            status,
            allocatorThrowing(admissionError),
            ignoreCorruptFiles)
        }
        assert(propagatedError eq admissionError)
      }

      val readErrorToIgnore = new RuntimeException("injected footer read error")
      assert(DeltaFileOperations.readParquetFooter(
        conf,
        status,
        allocatorThrowing(readErrorToIgnore),
        ignoreCorruptFiles = true).isEmpty)

      val readErrorToFail = new RuntimeException("injected footer read error")
      val wrappedError = intercept[DeltaIOException] {
        DeltaFileOperations.readParquetFooter(
          conf,
          status,
          allocatorThrowing(readErrorToFail),
          ignoreCorruptFiles = false)
      }
      assert(wrappedError.getErrorClass === "DELTA_FAILED_READ_FILE_FOOTER")
      assert(wrappedError.getCause eq readErrorToFail)
    }
  }

  test("REORG footer filtering evaluates in workers and isolates corrupt-file handling") {
    withTempDir { tempDir =>
      val parquetDir = new File(tempDir, "valid")
      spark.range(1).coalesce(1).write.parquet(parquetDir.getCanonicalPath)
      val conf = spark.sessionState.newHadoopConf()
      val parquetPath = new Path(parquetDir.toURI)
      val fs = parquetPath.getFileSystem(conf)
      val status = fs.listStatus(parquetPath)
        .find(_.getPath.getName.endsWith(".parquet")).get
      val admission = new DeltaFileOperations.ParquetFooterMemoryAdmission(
        memoryLimitBytes = 10 * 1024 * 1024,
        activeReaderLimit = 1,
        decodedObjectAllowanceBytes = 512 * 1024)
      val callingThread = Thread.currentThread().getName
      val predicateThread = new AtomicReference[String]()

      val selected = DeltaFileOperations.filterParquetFilesInParallel(
        conf,
        Seq("file" -> status),
        ignoreCorruptFiles = false,
        parallelism = 1,
        admission = admission) { (_, footer) =>
        predicateThread.set(Thread.currentThread().getName)
        footer.getParquetMetadata != null
      }
      assert(selected === Seq("file"))
      assert(predicateThread.get() !== callingThread)
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)

      val predicateFailure = new IllegalStateException("injected predicate failure")
      val predicateError = intercept[SparkException] {
        DeltaFileOperations.filterParquetFilesInParallel(
          conf,
          Seq("file" -> status),
          ignoreCorruptFiles = true,
          parallelism = 1,
          admission = admission) { (_, _) => throw predicateFailure }
      }
      val predicateCauses = Iterator.iterate[Throwable](predicateError)(_.getCause)
        .takeWhile(_ != null).toSeq
      assert(predicateCauses.exists(_ eq predicateFailure))
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)

      val fatalPredicateFailure = new OutOfMemoryError("injected fatal predicate failure")
      val propagatedFatal = intercept[OutOfMemoryError] {
        DeltaFileOperations.filterParquetFilesInParallel(
          conf,
          Seq("file" -> status),
          ignoreCorruptFiles = true,
          parallelism = 1,
          admission = admission) { (_, _) => throw fatalPredicateFailure }
      }
      assert(propagatedFatal eq fatalPredicateFailure)
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)

      val oversizedAdmission = new DeltaFileOperations.ParquetFooterMemoryAdmission(
        memoryLimitBytes = 1024,
        activeReaderLimit = 1,
        decodedObjectAllowanceBytes = 1024)
      val oversizedResult = DeltaFileOperations.filterParquetFilesInParallel(
        conf,
        Seq("file" -> status),
        ignoreCorruptFiles = true,
        parallelism = 1,
        admission = oversizedAdmission) { (_, _) => true }
      assert(oversizedResult === Seq("file"))
      assert(oversizedAdmission.availableMemoryBytes === 1024L)
      assert(oversizedAdmission.availableReaderPermits === 1)

      val corruptFile = new File(tempDir, "corrupt.parquet")
      Files.write(corruptFile.toPath, Array[Byte](1, 2, 3))
      val corruptStatus = fs.getFileStatus(new Path(corruptFile.toURI))
      val corruptResult = DeltaFileOperations.filterParquetFilesInParallel(
        conf,
        Seq("corrupt" -> corruptStatus),
        ignoreCorruptFiles = true,
        parallelism = 1,
        admission = admission) { (_, _) =>
        fail("Predicate must not run for a corrupt footer")
      }
      assert(corruptResult.isEmpty)
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)

      val corruptError = intercept[SparkException] {
        DeltaFileOperations.filterParquetFilesInParallel(
          conf,
          Seq("corrupt" -> corruptStatus),
          ignoreCorruptFiles = false,
          parallelism = 1,
          admission = admission) { (_, _) =>
          fail("Predicate must not run for a corrupt footer")
        }
      }
      val corruptCauses = Iterator.iterate[Throwable](corruptError)(_.getCause)
        .takeWhile(_ != null).toSeq
      assert(corruptCauses.exists {
        case e: DeltaIOException => e.getErrorClass == "DELTA_FAILED_READ_FILE_FOOTER"
        case _ => false
      })
      assert(admission.availableMemoryBytes === admission.memoryLimitBytes)
      assert(admission.availableReaderPermits === admission.activeReaderLimit)
    }
  }

  test("REORG footer scan parallelism matches ForkJoinPool bounds") {
    withSQLConf(
        DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_PARALLELISM.key -> Short.MaxValue.toString) {
      assert(spark.sessionState.conf
        .getConf(DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_PARALLELISM) === Short.MaxValue.toInt)
    }

    val error = intercept[IllegalArgumentException] {
      spark.conf.set(
        DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_PARALLELISM.key,
        (Short.MaxValue + 1).toString)
    }
    assert(error.getMessage.contains("must be between 1 and 32767"))
  }

  test("REORG PURGE rejects candidate path collisions across batches") {
    withTempDeltaTable(spark.range(0, 1, 1, numPartitions = 1).toDF()) { (_, log) =>
      val snapshot = log.update()
      val addFile = snapshot.allFiles.as[AddFile].head()
      val duplicateFiles = Seq(addFile, addFile)

      def assertCollision(block: => Unit): Unit = {
        val error = intercept[Exception](block)
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toSeq
        val classes = causes.map(_.getClass).mkString(" -> ")
        assert(causes.exists(_.isInstanceOf[AssertionError]),
          s"expected AssertionError, found $classes")
      }

      withSQLConf("spark.sql.leafNodeDefaultParallelism" -> "1") {
        Seq(0, 1).foreach { batchSize =>
          withSQLConf(
              DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_BATCH_SIZE.key -> batchSize.toString) {
            assertCollision {
              new DeltaPurgeOperation().filterFilesToReorg(spark, snapshot, duplicateFiles)
              ()
            }
          }
        }
      }
    }
  }

  test("Columns being renamed should not be purged") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5)
      .withColumn("id_before_rename", col("id") % 4)
      .withColumn("id_dropped", col("id") % 5)
      .toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      val (addFiles1, _) = getFileActionsInLastVersion(log)
      assert(addFiles1.size === 5)
      val footers1 = getParquetFooters(addFiles1, log)
      footers1.foreach { footer =>
        val fields = footer.getParquetMetadata.getFileMetaData.getSchema.getFields
        assert(fields.size == 3)
        assert(fields.toArray.map { _.toString }.contains("optional int64 id_dropped"))
        assert(fields.toArray.map { _.toString }.contains("optional int64 id_before_rename"))
      }

      // enable column-mapping first
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | SET TBLPROPERTIES (
           |   'delta.columnMapping.mode' = 'name'
           | )
           |""".stripMargin
      )
      // drop `id_dropped` and rename `id_before_rename` via alter table and run REORG PURGE,
      // this should remove `id_dropped` but keep `id_after_rename` in the parquet files.
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | DROP COLUMN id_dropped
           |""".stripMargin
      )
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | RENAME COLUMN id_before_rename TO id_after_rename
           |""".stripMargin
      )
      executePurge(path)

      val tableSchema = log.update().schema
      val tablePhysicalSchema = DeltaColumnMapping.renameColumns(tableSchema)
      val beforeRenameColStr = "StructField(id_before_rename,LongType,true)"
      val afterRenameColStr = "StructField(id_after_rename,LongType,true)"
      assert(tableSchema.fields.length == 2 &&
        tableSchema.map { _.toString }.contains(afterRenameColStr))
      assert(tablePhysicalSchema.fields.length == 2 &&
        tablePhysicalSchema.map { _.toString }.contains(beforeRenameColStr))

      val (addFiles2, _) = getFileActionsInLastVersion(log)
      assert(addFiles2.size === 1)
      val footers2 = getParquetFooters(addFiles2, log)
      footers2.foreach { footer =>
        val fields = footer.getParquetMetadata.getFileMetaData.getSchema.getFields
        assert(fields.size == 2)
        assert(!fields.toArray.map { _.toString }.contains("optional int64 id_dropped = 3"))
        // do note that the actual name for the column will not be
        // changed in parquet file level
        assert(fields.toArray.map { _.toString }.contains("optional int64 id_before_rename = 2"))
      }
    }
  }

  test("reorg on a catalog managed table should fail") {
    withCatalogManagedTable() { tableName =>
      spark.sql(s"INSERT INTO $tableName VALUES (1)")
      val snapshotBefore = getSnapshot(tableName)

      checkError(
        intercept[DeltaUnsupportedOperationException] {
          spark.sql(s"REORG TABLE $tableName APPLY (PURGE)")
        },
        "DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION",
        parameters = Map("operation" -> "DATA_REORGANIZATION")
      )

      val snapshotAfter = getSnapshot(tableName)
      assert(snapshotAfter.version === snapshotBefore.version)
      assert(snapshotAfter.allFiles.collect().toSet === snapshotBefore.allFiles.collect().toSet)
    }
  }
}
