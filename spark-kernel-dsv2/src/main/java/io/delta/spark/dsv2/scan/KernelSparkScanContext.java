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
package io.delta.spark.dsv2.scan;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.spark.dsv2.utils.ScalaUtils;
import io.delta.spark.dsv2.utils.SerializableReaderFunction;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Shared context for Kernel-based Spark scans that manages scan state and partition planning.
 *
 * <p>This class is created on the Driver and handles the conversion of Delta Kernel scan files into
 * Spark InputPartitions. It caches scan file batches to support multiple calls to planPartitions()
 * and ensures consistent partition planning.
 *
 * <p>This class serves as wrapper of kernel logic owned by {@link
 * io.delta.spark.dsv2.scan.KernelSparkScan} to handle the low-level operations of converting Kernel
 * scan files into Spark InputPartitions used by {@link
 * io.delta.spark.dsv2.scan.batch.KernelSparkBatchScan}.
 */
public class KernelSparkScanContext {

  private static final Logger LOG = LoggerFactory.getLogger(KernelSparkScanContext.class);

  private final Scan kernelScan;
  private final Engine engine;
  private final ParquetFileFormat parquetFileFormat;
  private final Configuration hadoopConf;
  private final StructType sparkReadSchema;

  /**
   * Cached scan file batches from kernel scan to avoid re-reading log files for replay on multiple
   * planPartitions calls
   */
  private final AtomicReference<Optional<List<FilteredColumnarBatch>>> cachedScanFileBatches;

  public KernelSparkScanContext(
      Scan kernelScan, StructType sparkReadSchema, Configuration hadoopConf) {
    this.engine = DefaultEngine.create(requireNonNull(hadoopConf, "hadoopConf is null"));
    this.kernelScan = requireNonNull(kernelScan, "kernelScan is null");
    this.parquetFileFormat = new ParquetFileFormat();
    this.cachedScanFileBatches = new AtomicReference<>(Optional.empty());
    this.hadoopConf = hadoopConf;
    this.sparkReadSchema = sparkReadSchema;
  }

  /**
   * Plans and creates input partitions for Spark execution.
   *
   * <p>This method converts Delta Kernel scan files into Spark InputPartitions. Each file row from
   * the kernel scan creates one input partition containing serialized scan state and file metadata.
   * The InputPartitions are serialized and sent from Driver to Executors.
   *
   * @return array of InputPartitions for Spark to process
   * @throws UncheckedIOException if kernel scan fails or partition creation fails
   */
  public InputPartition[] planPartitions() {
    final List<FilteredColumnarBatch> batches = loadScanFileBatches();
    return convertBatchesToInputPartitions(batches);
  }

  /**
   * Creates a reader function that can read data from partitioned files. This method handles the
   * creation of ParquetFileFormat and configures it with the appropriate schemas and configuration.
   *
   * @return A PreparableReadFunction that can read data from files
   */
  public SerializableReaderFunction createReaderFunction() {
    SparkSession sparkSession = SparkSession.active();
    Filter[] filtersArray = new Filter[0];
    Seq<Filter> filters =
        JavaConverters.asScalaBufferConverter(java.util.Arrays.asList(filtersArray))
            .asScala()
            .toSeq();
    Map<String, String> options = new HashMap<>();
    options.put(FileFormat.OPTION_RETURNING_BATCH(), String.valueOf(supportColumnar()));
    Function1<PartitionedFile, Iterator<InternalRow>> scalaReadFunc =
        parquetFileFormat.buildReaderWithPartitionValues(
            sparkSession,
            sparkReadSchema,
            new StructType(),
            sparkReadSchema,
            filters,
            ScalaUtils.toScalaMap(options),
            hadoopConf);
    return (PartitionedFile file) -> scalaReadFunc.apply(file);
  }

  public boolean supportColumnar() {
    return parquetFileFormat.supportBatch(SparkSession.active(), sparkReadSchema);
  }

  private List<FilteredColumnarBatch> loadScanFileBatches() {
    Optional<List<FilteredColumnarBatch>> batches = cachedScanFileBatches.get();
    if (!batches.isPresent()) {
      synchronized (this) {
        batches = cachedScanFileBatches.get();
        if (!batches.isPresent()) {
          List<FilteredColumnarBatch> batchList = new ArrayList<>();
          try (CloseableIterator<FilteredColumnarBatch> scanFiles =
              kernelScan.getScanFiles(engine)) {
            while (scanFiles.hasNext()) {
              FilteredColumnarBatch batch = scanFiles.next();
              batchList.add(batch);
            }
          } catch (IOException e) {
            LOG.warn("Failed to read from kernel scan", e);
            throw new UncheckedIOException("Failed to read added files from kernel scan", e);
          }
          batches = Optional.of(batchList);
          cachedScanFileBatches.set(batches);
        }
      }
    }
    return batches.get();
  }

  private InputPartition[] convertBatchesToInputPartitions(List<FilteredColumnarBatch> batches) {
    List<InputPartition> partitions = new ArrayList<>();
    for (FilteredColumnarBatch batch : batches) {
      try (CloseableIterator<Row> rows = batch.getRows()) {
        while (rows.hasNext()) {
          Row row = rows.next();
          PartitionedFile partitionedFile = partitionedFileFromKernelRow(row);
          partitions.add(
              new FilePartition(partitions.size(), new PartitionedFile[] {partitionedFile}));
        }
      } catch (IOException e) {
        LOG.warn("Failed to construct input partition", e);
        throw new UncheckedIOException("Failed to construct input partition", e);
      }
    }
    return partitions.toArray(new InputPartition[0]);
  }

  /**
   * Converts a Kernel Row representing a scan file to a Spark PartitionedFile. This method extracts
   * file information and partition values from the row.
   *
   * @param row The Kernel Row containing scan file information
   * @return A PartitionedFile that can be used by Spark's ParquetFileFormat
   */
  private PartitionedFile partitionedFileFromKernelRow(Row row) {
    FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(row);
    SparkPath filePath = SparkPath.fromPathString(fileStatus.getPath());
    long length = fileStatus.getSize();
    long modificationTime = fileStatus.getModificationTime();
    long start = 0;
    String[] hosts = new String[0];
    return new PartitionedFile(
        InternalRow.empty(),
        filePath, // File path
        start, // Start offset
        length, // File length
        hosts, // Host locations
        modificationTime, // Last modified time
        length, // File size
        ScalaUtils.toScalaMap(new HashMap<>())); // Metadata
  }
}
