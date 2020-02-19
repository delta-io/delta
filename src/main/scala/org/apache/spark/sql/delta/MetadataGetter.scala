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

import org.apache.spark.sql.delta.actions.{Metadata, Protocol, SetTransaction}

/**
 * MetadataGetter provides an interface to get the metadata information of a Delta log
 */
trait MetadataGetter extends ValidateChecksum {

  // For logging
  def deltaLog: DeltaLog
  protected val checksumOpt: Option[VersionChecksum]

  def protocol: Protocol
  def metadata: Metadata
  def setTransactions: Seq[SetTransaction]
  def sizeInBytes: Long
  def numOfFiles: Long
  def numOfMetadata: Long
  def numOfProtocol: Long
  def numOfRemoves: Long
  def numOfSetTransactions: Long
}
