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

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}

import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Unit tests for ProtocolMetadataAdapterV1.
 *
 * This suite tests the V1 wrapper implementation that adapts delta-spark's Protocol and Metadata
 * to the ProtocolMetadataAdapter interface.
 */
class ProtocolMetadataAdapterV1Suite extends ProtocolMetadataAdapterSuiteBase {

  override protected def createWrapper(
      minReaderVersion: Int = 1,
      minWriterVersion: Int = 2,
      readerFeatures: Option[Set[String]] = None,
      writerFeatures: Option[Set[String]] = None,
      schema: StructType = new StructType().add("id", IntegerType),
      configuration: Map[String, String] = Map.empty): ProtocolMetadataAdapter = {

    val protocol = Protocol(
      minReaderVersion = minReaderVersion,
      minWriterVersion = minWriterVersion,
      readerFeatures = readerFeatures,
      writerFeatures = writerFeatures)

    val metadata = Metadata(
      schemaString = schema.json,
      configuration = configuration)

    ProtocolMetadataAdapterV1(protocol, metadata)
  }
}
