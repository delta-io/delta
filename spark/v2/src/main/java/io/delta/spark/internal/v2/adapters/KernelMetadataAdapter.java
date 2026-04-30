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
package io.delta.spark.internal.v2.adapters;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.spark.sql.delta.DeltaColumnMappingMode;
import org.apache.spark.sql.delta.IdMapping$;
import org.apache.spark.sql.delta.NameMapping$;
import org.apache.spark.sql.delta.NoMapping$;
import org.apache.spark.sql.delta.v2.interop.AbstractMetadata;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Adapter from {@link io.delta.kernel.internal.actions.Metadata} to {@link
 * org.apache.spark.sql.delta.v2.interop.AbstractMetadata}.
 */
public class KernelMetadataAdapter implements AbstractMetadata {

  private final Metadata kernelMetadata;
  private volatile StructType cachedSchema;
  private volatile Seq<String> cachedPartitionColumns;
  private volatile Map<String, String> cachedConfiguration;
  private volatile StructType cachedPartitionSchema;

  public KernelMetadataAdapter(Metadata kernelMetadata) {
    this.kernelMetadata = Objects.requireNonNull(kernelMetadata, "kernelMetadata is null");
  }

  @Override
  public String id() {
    return kernelMetadata.getId();
  }

  @Override
  public String name() {
    return kernelMetadata.getName().orElse(null);
  }

  @Override
  public String description() {
    return kernelMetadata.getDescription().orElse(null);
  }

  @Override
  public StructType schema() {
    if (cachedSchema == null) {
      cachedSchema = SchemaUtils.convertKernelSchemaToSparkSchema(kernelMetadata.getSchema());
    }
    return cachedSchema;
  }

  @Override
  public Seq<String> partitionColumns() {
    if (cachedPartitionColumns == null) {
      cachedPartitionColumns =
          CollectionConverters.asScala(
                  VectorUtils.toJavaList(kernelMetadata.getPartitionColumns()).stream()
                      .map(Object::toString)
                      .collect(Collectors.toList()))
              .toSeq();
    }
    return cachedPartitionColumns;
  }

  @Override
  public Map<String, String> configuration() {
    if (cachedConfiguration == null) {
      cachedConfiguration = ScalaUtils.toScalaMap(kernelMetadata.getConfiguration());
    }
    return cachedConfiguration;
  }

  @Override
  public DeltaColumnMappingMode columnMappingMode() {
    ColumnMapping.ColumnMappingMode kernelMode =
        ColumnMapping.getColumnMappingMode(kernelMetadata.getConfiguration());
    switch (kernelMode) {
      case NONE:
        return NoMapping$.MODULE$;
      case ID:
        return IdMapping$.MODULE$;
      case NAME:
        return NameMapping$.MODULE$;
      default:
        throw new UnsupportedOperationException("Unsupported column mapping mode: " + kernelMode);
    }
  }

  @Override
  public StructType partitionSchema() {
    if (cachedPartitionSchema == null) {
      cachedPartitionSchema = AbstractMetadata.super.partitionSchema();
    }
    return cachedPartitionSchema;
  }
}
