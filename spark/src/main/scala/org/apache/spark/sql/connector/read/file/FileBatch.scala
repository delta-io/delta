/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.connector.read.file

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

/**
 * Marker [[Batch]] for [[FileScan]]. A correctly registered planner strategy must lower
 * the enclosing [[FileScan]] to a file-source physical plan before Spark's default DSv2
 * planner tries to materialize a `Batch`. If that fails, the methods below throw to make
 * the misconfiguration obvious.
 */
trait FileBatch extends Batch {

  final override def planInputPartitions(): Array[InputPartition] =
    throw new UnsupportedOperationException(
      "FileBatch is lowered at planning time; planInputPartitions must not be called.")

  final override def createReaderFactory(): PartitionReaderFactory =
    throw new UnsupportedOperationException(
      "FileBatch is lowered at planning time; createReaderFactory must not be called.")
}
