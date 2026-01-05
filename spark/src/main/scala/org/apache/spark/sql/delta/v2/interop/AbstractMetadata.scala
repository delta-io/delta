/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v2.interop

import org.apache.spark.sql.types.StructType

/**
 * Abstract trait for metadata actions in Delta. This trait provides a common
 * abstraction that can be implemented by both Spark's V1 Metadata and Kernel's MetadataV2
 * in V2 connector. The V2 connector will implement adapters for reusing V1 utilities.
 */
trait AbstractMetadata {

  /** A unique table identifier. */
  def id: String

  /** User-specified table identifier. */
  def name: String

  /** User-specified table description. */
  def description: String

  /** Returns the schema as a [[StructType]]. */
  def schema: StructType

  /** List of partition column names. */
  def partitionColumns: Seq[String]

  /** The table properties/configuration defined on the table. */
  def configuration: Map[String, String]
}

