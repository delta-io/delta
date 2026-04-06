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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FilePartition$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class SparkBatch implements Batch {
  private final Snapshot snapshot;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Predicate[] pushedToKernelFilters;
  private final Filter[] dataFilters;
  private final Set<String> canonicalKernelFilters;
  private final Set<String> canonicalDataFilters;
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
        pushedToKernelFilters != null
            ? Arrays.copyOf(pushedToKernelFilters, pushedToKernelFilters.length)
            : new Predicate[0];
    this.dataFilters =
        dataFilters != null ? Arrays.copyOf(dataFilters, dataFilters.length) : new Filter[0];
    this.canonicalKernelFilters =
        FilterComparisonUtils.canonicalPredicateSet(this.pushedToKernelFilters);
    this.canonicalDataFilters =
        FilterComparisonUtils.canonicalFilterSet(this.dataFilters);
    this.totalBytes = totalBytes;
    this.scalaOptions = Objects.requireNonNull(scalaOptions, "scalaOptions is null");
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf is null");
    this.sqlConf = SQLConf.get();
  }

  @Override
  public InputPartition[] planInputPartitions() {
    SparkSession sparkSession = SparkSession.active();
    long maxSplitBytes =
        PartitionUtils.calculateMaxSplitBytes(
            sparkSession, totalBytes, partitionedFiles.size(), sqlConf);

    // For non-partitioned tables, use simple file partitioning
    if (partitionSchema.fields().length == 0) {
      scala.collection.Seq<FilePartition> filePartitions =
          FilePartition$.MODULE$.getFilePartitions(
              sparkSession, JavaConverters.asScalaBuffer(partitionedFiles).toSeq(), maxSplitBytes);
      return JavaConverters.seqAsJavaList(filePartitions).toArray(new InputPartition[0]);
    }

    // For partitioned tables, group files by partition values and wrap in DeltaInputPartition
    // to support HasPartitionKey for KeyGroupedPartitioning optimizations
    return planPartitionedInputPartitions(sparkSession, maxSplitBytes);
  }

  /**
   * Plans input partitions for partitioned tables by grouping files by their partition values. Each
   * resulting DeltaInputPartition implements HasPartitionKey, enabling Spark to leverage partition
   * information for optimizations like shuffle elimination.
   */
  private InputPartition[] planPartitionedInputPartitions(
      SparkSession sparkSession, long maxSplitBytes) {
    // Note: Using InternalRow as map key relies on GenericInternalRow's value-based
    // equals()/hashCode(), which is what PartitionUtils.getPartitionRow() returns.
    Map<InternalRow, List<PartitionedFile>> filesByPartition = new LinkedHashMap<>();
    for (PartitionedFile file : partitionedFiles) {
      InternalRow partitionKey = file.partitionValues();
      filesByPartition.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(file);
    }

    // Create DeltaInputPartitions for each partition group
    List<InputPartition> result = new ArrayList<>();
    int partitionIndex = 0;

    for (Map.Entry<InternalRow, List<PartitionedFile>> entry : filesByPartition.entrySet()) {
      InternalRow partitionKey = entry.getKey();
      List<PartitionedFile> filesInPartition = entry.getValue();

      // Split files within this partition based on maxSplitBytes
      scala.collection.Seq<FilePartition> filePartitions =
          FilePartition$.MODULE$.getFilePartitions(
              sparkSession, JavaConverters.asScalaBuffer(filesInPartition).toSeq(), maxSplitBytes);

      // Wrap each FilePartition in a DeltaInputPartition with the partition key.
      // Re-index partitions with a global counter because getFilePartitions returns 0-based
      // indices within each partition group, but we need unique indices across all groups.
      for (FilePartition fp : JavaConverters.seqAsJavaList(filePartitions)) {
        FilePartition reindexedPartition = new FilePartition(partitionIndex++, fp.files());
        result.add(new DeltaInputPartition(reindexedPartition, partitionKey));
      }
    }

    return result.toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return PartitionUtils.createDeltaParquetReaderFactory(
        snapshot,
        dataSchema,
        partitionSchema,
        readDataSchema,
        dataFilters,
        scalaOptions,
        hadoopConf,
        sqlConf);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof SparkBatch)) return false;

    SparkBatch that = (SparkBatch) obj;
    return Objects.equals(this.snapshot.getPath(), that.snapshot.getPath())
        && this.snapshot.getVersion() == that.snapshot.getVersion()
        && Objects.equals(this.readDataSchema, that.readDataSchema)
        && Objects.equals(this.dataSchema, that.dataSchema)
        && Objects.equals(this.partitionSchema, that.partitionSchema)
        && this.canonicalKernelFilters.equals(that.canonicalKernelFilters)
        && this.canonicalDataFilters.equals(that.canonicalDataFilters)
        && partitionedFiles.size() == that.partitionedFiles.size();
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(snapshot.getPath(), snapshot.getVersion());
    result = 31 * result + readDataSchema.hashCode();
    result = 31 * result + dataSchema.hashCode();
    result = 31 * result + partitionSchema.hashCode();
    result = 31 * result + canonicalKernelFilters.hashCode();
    result = 31 * result + canonicalDataFilters.hashCode();
    result = 31 * result + Integer.hashCode(partitionedFiles.size());
    return result;
  }
}
