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
package io.delta.kernel.spark.read;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FilePartition$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

public class SparkBatch implements Batch {
  private final String tablePath;
  private final org.apache.spark.sql.types.StructType readDataSchema;
  private final org.apache.spark.sql.types.StructType dataSchema;
  private final org.apache.spark.sql.types.StructType partitionSchema;
  private final Predicate[] pushedToKernelFilters;
  private final Filter[] dataFilters;
  private final Configuration hadoopConf;
  private final SQLConf sqlConf;
  private final long totalBytes;
  private scala.collection.immutable.Map<String, String> scalaOptions;
  private final List<PartitionedFile> partitionedFiles;
  private final SnapshotImpl snapshot;
  private final Engine kernelEngine;

  public SparkBatch(
      Engine kernelEngine,
      String tablePath,
      org.apache.spark.sql.types.StructType dataSchema,
      org.apache.spark.sql.types.StructType partitionSchema,
      org.apache.spark.sql.types.StructType readDataSchema,
      List<PartitionedFile> partitionedFiles,
      Predicate[] pushedToKernelFilters,
      Filter[] dataFilters,
      long totalBytes,
      scala.collection.immutable.Map<String, String> scalaOptions,
      Configuration hadoopConf,
      SnapshotImpl snapshot) {

    this.kernelEngine = Objects.requireNonNull(kernelEngine, "kernelEngine is null");
    this.tablePath = Objects.requireNonNull(tablePath, "tableName is null");
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
    this.totalBytes = totalBytes;
    this.scalaOptions = Objects.requireNonNull(scalaOptions, "scalaOptions is null");
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf is null");
    this.snapshot = Objects.requireNonNull(snapshot, "snapshot is null");
    this.sqlConf = SQLConf.get();
  }

  @Override
  public InputPartition[] planInputPartitions() {
    SparkSession sparkSession = SparkSession.active();
    long maxSplitBytes = calculateMaxSplitBytes(sparkSession);

    scala.collection.Seq<FilePartition> filePartitions =
        FilePartition$.MODULE$.getFilePartitions(
            sparkSession, JavaConverters.asScalaBuffer(partitionedFiles).toSeq(), maxSplitBytes);
    return JavaConverters.seqAsJavaList(filePartitions).toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    boolean enableVectorizedReader =
        ParquetUtils.isBatchReadSupportedForSchema(sqlConf, readDataSchema);
    scala.collection.immutable.Map<String, String> optionsWithBatch =
        scalaOptions.$plus(
            new Tuple2<>(
                FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                String.valueOf(enableVectorizedReader)));

    Protocol protocol = snapshot.getProtocol();
    Metadata metadata = snapshot.getMetadata();

    Function1<PartitionedFile, Iterator<InternalRow>> readFunc =
        new DeltaParquetFileFormat(kernelEngine, protocol, metadata, tablePath)
            .buildReaderWithPartitionValues(
                SparkSession.active(),
                dataSchema,
                partitionSchema,
                readDataSchema,
                JavaConverters.asScalaBuffer(Arrays.asList(dataFilters)).toSeq(),
                optionsWithBatch,
                hadoopConf);

    return new SparkReaderFactory(readFunc, enableVectorizedReader);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof SparkBatch)) return false;

    SparkBatch that = (SparkBatch) obj;
    return Objects.equals(this.tablePath, that.tablePath)
        && Objects.equals(this.readDataSchema, that.readDataSchema)
        && Objects.equals(this.partitionSchema, that.partitionSchema)
        && Arrays.equals(this.pushedToKernelFilters, that.pushedToKernelFilters)
        && Arrays.equals(this.dataFilters, that.dataFilters)
        && partitionedFiles.size() == that.partitionedFiles.size();
  }

  @Override
  public int hashCode() {
    int result = tablePath.hashCode();
    result = 31 * result + readDataSchema.hashCode();
    result = 31 * result + partitionSchema.hashCode();
    result = 31 * result + Arrays.hashCode(pushedToKernelFilters);
    result = 31 * result + Arrays.hashCode(dataFilters);
    result = 31 * result + Integer.hashCode(partitionedFiles.size());
    return result;
  }

  private long calculateMaxSplitBytes(SparkSession sparkSession) {
    long defaultMaxSplitBytes = sqlConf.filesMaxPartitionBytes();
    long openCostInBytes = sqlConf.filesOpenCostInBytes();
    Option<Object> minPartitionNumOption = sqlConf.filesMinPartitionNum();

    int minPartitionNum =
        minPartitionNumOption.isDefined()
            ? ((Number) minPartitionNumOption.get()).intValue()
            : sqlConf
                .getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM())
                .getOrElse(() -> sparkSession.sparkContext().defaultParallelism());
    if (minPartitionNum <= 0) {
      minPartitionNum = 1;
    }
    long calculatedTotalBytes = totalBytes + (long) partitionedFiles.size() * openCostInBytes;
    long bytesPerCore = calculatedTotalBytes / minPartitionNum;

    return Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore));
  }
}
