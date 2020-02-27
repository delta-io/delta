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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.{Metadata, Protocol, SetTransaction, SingleAction}
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{coalesce, collect_set, count, last, lit, sum, udf}

class StateMetadataGetter(
    spark: SparkSession,
    state: Dataset[SingleAction],
    override val deltaLog: DeltaLog,
    override protected val checksumOpt: Option[VersionChecksum]) extends MetadataGetter {

  import StateMetadataGetter._

  override def protocol: Protocol = materializedState.protocol
  override def metadata: Metadata = materializedState.metadata
  override def setTransactions: Seq[SetTransaction] = materializedState.setTransactions
  override def sizeInBytes: Long = materializedState.sizeInBytes
  override def numOfFiles: Long = materializedState.numOfFiles
  override def numOfMetadata: Long = materializedState.numOfMetadata
  override def numOfProtocol: Long = materializedState.numOfProtocol
  override def numOfRemoves: Long = materializedState.numOfRemoves
  override def numOfSetTransactions: Long = materializedState.numOfSetTransactions

  private lazy val materializedState: State = {
    val implicits = spark.implicits
    import implicits._
    state.select(
      coalesce(last($"protocol", ignoreNulls = true), defaultProtocol()) as "protocol",
      coalesce(last($"metaData", ignoreNulls = true), emptyMetadata()) as "metadata",
      collect_set($"txn") as "setTransactions",
      // sum may return null for empty data set.
      coalesce(sum($"add.size"), lit(0L)) as "sizeInBytes",
      count($"add") as "numOfFiles",
      count($"metaData") as "numOfMetadata",
      count($"protocol") as "numOfProtocol",
      count($"remove") as "numOfRemoves",
      count($"txn") as "numOfSetTransactions")
      .as[State](stateEncoder)
  }.first()
}

object StateMetadataGetter extends DeltaLogging {

  private case class State(
    protocol: Protocol,
    metadata: Metadata,
    setTransactions: Seq[SetTransaction],
    sizeInBytes: Long,
    numOfFiles: Long,
    numOfMetadata: Long,
    numOfProtocol: Long,
    numOfRemoves: Long,
    numOfSetTransactions: Long)

  private[this] lazy val _stateEncoder: ExpressionEncoder[State] = try {
    ExpressionEncoder[State]()
  } catch {
    case e: Throwable =>
      logError(e.getMessage, e)
      throw e
  }


  private lazy val emptyMetadata = udf(() => Metadata())
  private lazy val defaultProtocol = udf(() => Protocol())
  implicit private def stateEncoder: Encoder[State] = {
    _stateEncoder.copy()
  }
}

class EmptyMetadataGetter(
    override val metadata: Metadata,
    override val deltaLog: DeltaLog) extends MetadataGetter {
  override protected val checksumOpt: Option[VersionChecksum] = None
  override def protocol: Protocol = Protocol()
  override def setTransactions: Seq[SetTransaction] = Nil
  override def sizeInBytes: Long = 0
  override def numOfFiles: Long = 0
  override def numOfMetadata: Long = 0
  override def numOfProtocol: Long = 0
  override def numOfRemoves: Long = 0
  override def numOfSetTransactions: Long = 0
}
