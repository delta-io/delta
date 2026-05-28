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

package org.apache.spark.sql.connector.write.file

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.write.{BatchWrite, PhysicalWriteInfo, Write}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.StructType

/**
 * A [[Write]] backed by a [[FileFormat]], intended to be lowered to connector-defined V2 write
 * exec nodes (e.g. AppendFilesExec) at planning time.
 *
 * The connector contributes a planner strategy that matches the relevant V2 write commands
 * (AppendData / OverwriteByExpression / OverwritePartitionsDynamic / streaming counterparts)
 * over a table whose `newWriteBuilder` returns a [[FileWrite]], and constructs the matching
 * physical exec node. The exec node calls back into [[newFileOutputWriter]] on the driver to
 * open a transaction, write the rows, and commit.
 *
 * The default `toBatch` / `toStreaming` inherited from [[Write]] are overridden to throw -
 * a correctly registered strategy must consume the [[FileWrite]] before Spark's default DSv2
 * planner reaches it.
 */
trait FileWrite extends Write {

  /** The output schema (post-projection). */
  def writeSchema(): StructType

  /** The underlying [[FileFormat]]. Exposed so Photon can swap the exec node 1:1. */
  def fileFormat(): FileFormat

  /** The partition column names in storage order. Empty for non-partitioned tables. */
  def partitionColumns(): Seq[String]

  /**
   * Constraint predicates to evaluate before writing. Each expression is expected to evaluate
   * to a non-false boolean; rows that violate any constraint cause the write to fail. The
   * connector translates its own constraint representation (e.g. Delta CHECK constraints) into
   * Spark expressions here so the exec node can enforce them natively via a `FilterExec`-style
   * projection.
   */
  def constraints(): Seq[Expression]

  /**
   * Generated-column expressions to inject above the user query before writing. The exec node
   * adds a `ProjectExec`-style operator that replaces any user-supplied value with the
   * generated expression for the listed column names.
   */
  def generatedColumnExpressions(): Map[String, Expression]

  /**
   * Driver-side writer factory. Called once per write from the connector's V2 write exec node;
   * the returned writer owns the lifecycle of the open transaction (open / write rows /
   * commit / abort).
   */
  def newFileOutputWriter(info: PhysicalWriteInfo): FileOutputWriter

  final override def toBatch: BatchWrite =
    throw new UnsupportedOperationException(
      "FileWrite is lowered at planning time; toBatch must not be called.")

  final override def toStreaming: StreamingWrite =
    throw new UnsupportedOperationException(
      "FileWrite is lowered at planning time; toStreaming must not be called.")
}
