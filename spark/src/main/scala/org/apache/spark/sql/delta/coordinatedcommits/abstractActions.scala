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

package org.apache.spark.sql.delta.coordinatedcommits

/**
 * Interface for protocol actions in Delta. The protocol defines the requirements
 * that readers and writers of the table need to meet.
 */
trait AbstractProtocol {
  /** The minimum reader version required to read the table. */
  def getMinReaderVersion: Int
  /** The minimum writer version required to read the table. */
  def getMinWriterVersion: Int
  /** The reader features that need to be supported to read the table. */
  def getReaderFeatures: Option[Set[String]]
  /** The writer features that need to be supported to write the table. */
  def getWriterFeatures: Option[Set[String]]
}

/**
 * Interface for metadata actions in Delta. The metadata defines any metadata
 * that can be set on a table.
 */
trait AbstractMetadata {
  /** A unique table identifier. */
  def getId: String
  /** User-specified table identifier. */
  def getName: String
  /** User-specified table description. */
  def getDescription: String
  /** The table provider format. */
  def getProvider: String
  /** The format options */
  def getFormatOptions: Map[String, String]
  /** The table schema in string representation. */
  def getSchemaString: String
  /** List of partition columns. */
  def getPartitionColumns: Seq[String]
  /** The table properties defined on the table. */
  def getConfiguration: Map[String, String]
  /** Timestamp for the creation of this metadata. */
  def getCreatedTime: Option[Long]
}

/**
 * Interface for commit info actions in Delta. The commit info at the least needs
 * to provide a commit timestamp to specify when the commit happened.
 */
trait AbstractCommitInfo {
  def getCommitTimestamp: Long
}
