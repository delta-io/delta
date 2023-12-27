/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import io.delta.golden.GoldenTableUtils.goldenTablePath
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import io.delta.kernel.client.{FileReadContext, JsonHandler, ParquetHandler, TableClient}
import io.delta.kernel.data.{FileDataReadResult, FilteredColumnarBatch}
import io.delta.kernel.expressions.{Column, Literal, Predicate}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator
import io.delta.kernel.{Snapshot, Table}

import io.delta.kernel.defaults.client.{DefaultJsonHandler, DefaultParquetHandler, DefaultTableClient}
import io.delta.kernel.defaults.utils.TestUtils

class ScanSuite extends AnyFunSuite with TestUtils {
  import io.delta.kernel.defaults.ScanSuite._

  test("conditionally read the statistics column based on the query filter") {
    // For now we read the stats column whenever there is a data predicate, this test will need to
    // be updated once we actually generate data filters and use the column statistics

    // Remove this golden table when updating this test to read real stats
    val path = goldenTablePath("conditionally-read-add-stats")
    val tableClient = tableClientDisallowedStatsReads

    def snapshot(tableClient: TableClient): Snapshot = {
      Table.forPath(tableClient, path).getLatestSnapshot(tableClient)
    }

    def verifyNoStatsColumn(scanFiles: CloseableIterator[FilteredColumnarBatch]): Unit = {
      scanFiles.forEach { batch =>
        val addSchema = batch.getData.getSchema.get("add").getDataType.asInstanceOf[StructType]
        assert(addSchema.indexOf("stats") < 0)
      }
    }

    // no filter --> don't read stats
    verifyNoStatsColumn(
      snapshot(tableClientDisallowedStatsReads)
        .getScanBuilder(tableClient).build()
        .getScanFiles(tableClient))

    // partition filter only --> don't read stats
    val partFilter = new Predicate("=", new Column("part"), Literal.ofInt(1))
    verifyNoStatsColumn(
      snapshot(tableClientDisallowedStatsReads)
        .getScanBuilder(tableClient).withFilter(tableClient, partFilter).build()
        .getScanFiles(tableClient))

    // data filter --> do read stats
    val dataFilter = new Predicate("=", new Column("id"), Literal.ofInt(1))
    collectScanFileRows(
      snapshot(defaultTableClient)
        .getScanBuilder(defaultTableClient).withFilter(defaultTableClient, dataFilter).build(),
      defaultTableClient
    ).foreach { row =>
      assert(row.getStruct(0).getString(6) == "fake_statistics_string")
    }
  }
}

object ScanSuite {

  private def throwErrorIfAddStatsInSchema(readSchema: StructType): Unit = {
    if (readSchema.indexOf("add") >= 0) {
      val addSchema = readSchema.get("add").getDataType.asInstanceOf[StructType]
      assert(addSchema.indexOf("stats") < 0, "reading column add.stats is not allowed");
    }
  }

  /**
   * Returns a custom table client implementation that doesn't allow "add.stats" in the read schema
   * for parquet or json handlers.
   */
  def tableClientDisallowedStatsReads: TableClient = {
    val hadoopConf = new Configuration()
    new DefaultTableClient(hadoopConf) {

      override def getParquetHandler: ParquetHandler = {
        new DefaultParquetHandler(hadoopConf) {
          override def readParquetFiles(
            fileIter: CloseableIterator[FileReadContext],
            physicalSchema: StructType): CloseableIterator[FileDataReadResult] = {
            throwErrorIfAddStatsInSchema(physicalSchema)
            super.readParquetFiles(fileIter, physicalSchema)
          }
        }
      }

      override def getJsonHandler: JsonHandler = {
        new DefaultJsonHandler(hadoopConf) {
          override def readJsonFiles(
            fileIter: CloseableIterator[FileReadContext],
            physicalSchema: StructType): CloseableIterator[FileDataReadResult] = {
            throwErrorIfAddStatsInSchema(physicalSchema)
            super.readJsonFiles(fileIter, physicalSchema)
          }
        }
      }
    }
  }
}
