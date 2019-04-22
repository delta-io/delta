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

import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils}

import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamTest}

trait DeltaOSSTestUtils extends StreamTest {
  /**
   * DataFrameWriter doesn't pass partitioning information to the data source v1 API, so we have to
   * use this implicit trick to get it.
   * DataStreamWriter does pass partitioning, but we do the same thing to avoid having to reason
   * about which cases require partitionBy vs partitionByHack.
   */
  implicit class BetterFrameWriter[A](dfw: DataFrameWriter[A]) {
    def partitionByHack(columns: String*): DataFrameWriter[A] = {
      dfw.option(
        DeltaSourceUtils.PARTITIONING_COLUMNS_KEY,
        DeltaDataSource.encodePartitioningColumns(columns))
    }
  }

  implicit class BetterStreamWriter[A](dsw: DataStreamWriter[A]) {
    def partitionByHack(columns: String*): DataStreamWriter[A] = {
      dsw.partitionBy(columns: _*)
    }
  }
}
