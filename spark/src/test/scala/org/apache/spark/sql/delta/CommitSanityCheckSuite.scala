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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.DeltaTestUtils.{filterUsageRecords, BOOLEAN_DOMAIN}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for AddFile sanity checks during commit:
 * - Empty (0-byte) parquet file detection
 * - Null partition value validation for NOT NULL columns
 */
class CommitSanityCheckSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_EMPTY_FILE_CHECK_THROW_ENABLED.key, "true")
      .set(DeltaSQLConf.DELTA_NULL_PARTITION_CHECK_THROW_ENABLED.key, "true")
  }

  private def createTable(tempDir: java.io.File): DeltaLog = {
    sql(s"CREATE TABLE delta.`${tempDir.getCanonicalPath}` " +
      s"(id Long, value String) USING delta")
    DeltaLog.forTable(spark, tempDir.getCanonicalPath)
  }

  /**
   * Creates a partitioned table with a NOT NULL partition column and returns the DeltaLog.
   */
  private def createPartitionedTableWithNotNullColumn(tempDir: java.io.File): DeltaLog = {
    sql(s"CREATE TABLE delta.`${tempDir.getCanonicalPath()}` " +
      s"(part String NOT NULL, value Int) using delta PARTITIONED BY (part)" )
    DeltaLog.forTable(spark, tempDir.getCanonicalPath())
  }

  private def commit(
    deltaLog: DeltaLog, addFile: AddFile, isCommitLarge: Boolean): Unit = {
    val txn = deltaLog.startTransaction()
    if (isCommitLarge) {
      txn.commitLarge(
        spark,
        Seq(addFile).toIterator,
        newProtocolOpt = None,
        op = DeltaOperations.ManualUpdate,
        context = Map.empty,
        metrics = Map.empty
      )
    } else {
      txn.commit(Seq(addFile), DeltaOperations.ManualUpdate)
    }
  }

  // ---------------------------------------------------------------------------
  // Empty file checks
  // ---------------------------------------------------------------------------

  BOOLEAN_DOMAIN.foreach { isCommitLarge =>
    test(s"detect zero-byte AddFile [isCommitLarge: $isCommitLarge]") {
      withTempDir { tempDir =>
        val deltaLog = createTable(tempDir)

        val addFile = AddFile(
          path = "part-00000.parquet",
          partitionValues = Map.empty,
          size = 0,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = """{"numRecords": 0}"""
        )

        val e = intercept[IllegalStateException] {
          commit(deltaLog, addFile, isCommitLarge)
        }
        assert(e.getMessage.contains("zero-byte"))
        assert(e.getMessage.contains("part-00000.parquet"))
      }
    }

    test(s"no error when file size is positive [isCommitLarge: $isCommitLarge]") {
      withTempDir { tempDir =>
        val deltaLog = createTable(tempDir)

        val addFile = AddFile(
          path = "part-00000.parquet",
          partitionValues = Map.empty,
          size = 100,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = """{"numRecords": 1}"""
        )

        // Should not throw
        commit(deltaLog, addFile, isCommitLarge)
      }
    }

    test(s"empty file check - only log when throw is disabled " +
        s"[isCommitLarge: $isCommitLarge]") {
      withTempDir { tempDir =>
        withSQLConf(DeltaSQLConf.DELTA_EMPTY_FILE_CHECK_THROW_ENABLED.key -> "false") {
          val deltaLog = createTable(tempDir)

          val addFile = AddFile(
            path = "part-00000.parquet",
            partitionValues = Map.empty,
            size = 0,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            stats = """{"numRecords": 0}"""
          )

          // Should not throw when flag is disabled, only log
          val events = Log4jUsageLogger.track {
            commit(deltaLog, addFile, isCommitLarge)
          }

          val violationEvents =
            filterUsageRecords(events, "delta.sanityCheck.emptyParquetFile")
          assert(violationEvents.size == 1)

          val eventBlob = JsonUtils.fromJson[Map[String, Any]](violationEvents.head.blob)
          assert(eventBlob.contains("addFile"))
          assert(eventBlob.contains("stackTrace"))
        }
      }
    }

    // ---------------------------------------------------------------------------
    // Null partition checks
    // ---------------------------------------------------------------------------

    test(s"detect null partition value for NOT NULL column with column mapping " +
        s"[isCommitLarge: $isCommitLarge]") {
      withTempDir { tempDir =>
        sql(s"CREATE TABLE delta.`${tempDir.getCanonicalPath()}` " +
          s"(part String NOT NULL, value Int) USING delta PARTITIONED BY (part) " +
          s"TBLPROPERTIES('delta.columnMapping.mode'='name')")
        val deltaLog = DeltaLog.forTable(spark, tempDir.getCanonicalPath())
        val physicalPartCol = deltaLog.snapshot.metadata.physicalPartitionColumns.head

        val addFile = AddFile(
          path = s"$physicalPartCol=__HIVE_DEFAULT_PARTITION__/file.parquet",
          partitionValues = Map(physicalPartCol -> null),
          size = 100,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = """{"numRecords": 1}"""
        )

        val e = intercept[IllegalStateException] {
          commit(deltaLog, addFile, isCommitLarge)
        }
        assert(e.getMessage.contains("null partition value"))
        assert(e.getMessage.contains(s"NOT NULL column '$physicalPartCol'"))
      }
    }

    test(s"detect null partition value for NOT NULL column [isCommitLarge: $isCommitLarge]") {
      withTempDir { tempDir =>
        val deltaLog = createPartitionedTableWithNotNullColumn(tempDir)

        // Create an AddFile with null partition value
        val addFile = AddFile(
          path = "part=__HIVE_DEFAULT_PARTITION__/file.parquet",
          partitionValues = Map("part" -> null),
          size = 100,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = """{"numRecords": 1}"""
        )

        val e = intercept[IllegalStateException] {
          commit(deltaLog, addFile, isCommitLarge)
        }
        assert(e.getMessage.contains("null partition value"))
        assert(e.getMessage.contains("NOT NULL column 'part'"))
      }
    }

    test(s"no error when partition value is not null [isCommitLarge: $isCommitLarge]") {
      withTempDir { tempDir =>
        val deltaLog = createPartitionedTableWithNotNullColumn(tempDir)

        // Create an AddFile with valid (non-null) partition value
        val addFile = AddFile(
          path = "part=valid_value/file.parquet",
          partitionValues = Map("part" -> "valid_value"),
          size = 100,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = """{"numRecords": 1}"""
        )

        // Should not throw
        commit(deltaLog, addFile, isCommitLarge)
      }
    }

    test(s"null partition check - only log when throw is disabled " +
        s"[isCommitLarge: $isCommitLarge]") {
      withTempDir { tempDir =>
        withSQLConf(DeltaSQLConf.DELTA_NULL_PARTITION_CHECK_THROW_ENABLED.key -> "false") {
          val deltaLog = createPartitionedTableWithNotNullColumn(tempDir)

          // Create an AddFile with null partition value
          val addFile = AddFile(
            path = "part=__HIVE_DEFAULT_PARTITION__/file.parquet",
            partitionValues = Map("part" -> null),
            size = 100,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            stats = """{"numRecords": 1}"""
          )

          // Should not throw when flag is disabled, only log
          val events = Log4jUsageLogger.track {
            commit(deltaLog, addFile, isCommitLarge)
          }

          // Validate that the null partition violation event was emitted
          val violationEvents =
            filterUsageRecords(events, "delta.constraints.nullPartitionViolation")
          assert(violationEvents.size == 1)

          val eventBlob = JsonUtils.fromJson[Map[String, Any]](violationEvents.head.blob)
          assert(eventBlob.contains("addFile"))
          assert(eventBlob.contains("notNullPartitionCols"))
          assert(eventBlob("notNullPartitionCols").toString == "part")
          assert(eventBlob.contains("stackTrace"))
        }
      }
    }
  }
}
