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

package org.apache.spark.sql.delta.v2.interop

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.actions.{Metadata => KernelMetadata}
import io.delta.kernel.internal.actions.{Protocol => KernelProtocol}
import io.delta.kernel.internal.util.VectorUtils

/**
 * Bridges Kernel's actions ([[KernelMetadata]] / [[KernelProtocol]]) to V1 Delta actions
 * ([[Metadata]] / [[Protocol]]). Used by `DeltaV2Snapshot` to expose Kernel snapshot
 * data through the V1 Snapshot contract.
 */
private[delta] object KernelSnapshotConversionsUtils {

  def metadataFromKernel(metadata: KernelMetadata): Metadata = {
    Metadata(
      id = metadata.getId,
      name = metadata.getName.orElse(null),
      description = metadata.getDescription.orElse(null),
      format = Format(
        provider = metadata.getFormat.getProvider,
        options = metadata.getFormat.getOptions.asScala.toMap),
      schemaString = metadata.getSchemaString,
      // getPartitionColumns preserves the declared order and original case of partition
      // columns; getPartitionColNames would return an unordered set of lowercased names.
      partitionColumns =
        VectorUtils.toJavaList[String](metadata.getPartitionColumns).asScala.toSeq,
      configuration = metadata.getConfiguration.asScala.toMap,
      createdTime =
        if (metadata.getCreatedTime.isPresent) Some(metadata.getCreatedTime.get.longValue())
        else None)
  }

  def protocolFromKernel(protocol: KernelProtocol): Protocol = {
    val readerFeatures =
      Option(protocol.getReaderFeatures).map(_.asScala.toSet).getOrElse(Set.empty)
    val writerFeatures =
      Option(protocol.getWriterFeatures).map(_.asScala.toSet).getOrElse(Set.empty)

    Protocol(protocol.getMinReaderVersion, protocol.getMinWriterVersion)
      .withWriterFeatures(writerFeatures)
      .withReaderFeatures(readerFeatures)
  }
}
