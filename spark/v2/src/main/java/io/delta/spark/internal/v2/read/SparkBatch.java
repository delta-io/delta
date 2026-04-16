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
package io.delta.spark.internal.v2.read;

import io.delta.kernel.Snapshot;
import io.delta.kernel.expressions.Predicate;
import io.delta.spark.internal.v2.utils.PartitionUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class SparkBatch implements Batch {
  private final Snapshot snapshot;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Predicate[] pushedToKernelFilters;
  private final Filter[] dataFilters;
  // Derived Sets used only for equals/hashCode: filters are AND-ed at eval time,
  // so list order has no semantic meaning.
  private final Set<Predicate> pushedToKernelFiltersSet;
  private final Set<Filter> dataFiltersSet;
  private final Configuration hadoopConf;
  private final SQLConf sqlConf;
  private final long totalBytes;
  private scala.collection.immutable.Map<String, String> scalaOptions;
  private final List<PartitionedFile> partitionedFiles;

  public SparkBatch(
      Snapshot snapshot,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      List<PartitionedFile> partitionedFiles,
      Predicate[] pushedToKernelFilters,
      Filter[] dataFilters,
      long totalBytes,
      scala.collection.immutable.Map<String, String> scalaOptions,
      Configuration hadoopConf) {

    this.snapshot = Objects.requireNonNull(snapshot, "snapshot is null");
    this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = Objects.requireNonNull(partitionSchema, "partitionSchema is null");
    this.readDataSchema = Objects.requireNonNull(readDataSchema, "readDataSchema is null");
    this.partitionedFiles =
        java.util.Collections.unmodifiableList(
            new ArrayList<>(Objects.requireNonNull(partitionedFiles, "partitionedFiles is null")));
    this.pushedToKernelFilters =
        pushedToKernelFilters == null ? new Predicate[0] : pushedToKernelFilters.clone();
    this.dataFilters = dataFilters == null ? new Filter[0] : dataFilters.clone();
    this.pushedToKernelFiltersSet = Set.copyOf(Arrays.asList(this.pushedToKernelFilters));
    this.dataFiltersSet = Set.copyOf(Arrays.asList(this.dataFilters));
    this.totalBytes = totalBytes;
    this.scalaOptions = Objects.requireNonNull(scalaOptions, "scalaOptions is null");
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf is null");
    this.sqlConf = SQLConf.get();
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return PartitionUtils.planInputPartitions(
        SparkSession.active(), partitionedFiles, totalBytes, hadoopConf, sqlConf);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    // TODO: support write-time CDF on batch reads.
    return PartitionUtils.createDeltaParquetReaderFactory(
        snapshot,
        dataSchema,
        partitionSchema,
        readDataSchema,
        dataFilters,
        scalaOptions,
        hadoopConf,
        sqlConf,
        /* isCDCRead */ false);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof SparkBatch)) return false;

    SparkBatch that = (SparkBatch) obj;
    return Objects.equals(this.snapshot, that.snapshot)
        && Objects.equals(this.readDataSchema, that.readDataSchema)
        && Objects.equals(this.dataSchema, that.dataSchema)
        && Objects.equals(this.partitionSchema, that.partitionSchema)
        && Objects.equals(this.pushedToKernelFiltersSet, that.pushedToKernelFiltersSet)
        && Objects.equals(this.dataFiltersSet, that.dataFiltersSet)
        && partitionedFiles.size() == that.partitionedFiles.size();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        snapshot,
        readDataSchema,
        dataSchema,
        partitionSchema,
        pushedToKernelFiltersSet,
        dataFiltersSet,
        partitionedFiles.size());
  }
}
