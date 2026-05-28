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

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}

/**
 * Marker [[MicroBatchStream]] for streaming reads backed by files. The enclosing
 * [[FileScan]] / streaming relation must be lowered by a connector-side planner strategy
 * before the engine asks for partitions or offsets; the methods below throw if reached.
 *
 * Subclasses still implement [[initialOffset]] / [[deserializeOffset]] / [[commit]] /
 * [[stop]] from [[org.apache.spark.sql.connector.read.streaming.SparkDataStream]], so the
 * relation can carry the connector's offset state through analysis/optimization for the
 * strategy to inspect.
 */
trait FileMicroBatchStream extends MicroBatchStream {

  final override def latestOffset(): Offset =
    throw new UnsupportedOperationException(
      "FileMicroBatchStream is lowered at planning time; latestOffset must not be called.")

  final override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] =
    throw new UnsupportedOperationException(
      "FileMicroBatchStream is lowered at planning time; planInputPartitions must not be called.")

  final override def createReaderFactory(): PartitionReaderFactory =
    throw new UnsupportedOperationException(
      "FileMicroBatchStream is lowered at planning time; createReaderFactory must not be called.")
}
