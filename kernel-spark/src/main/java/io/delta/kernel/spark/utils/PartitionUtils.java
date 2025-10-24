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
package io.delta.kernel.spark.utils;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.spark.read.SparkReaderFactory;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.PartitioningUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

/** Utility class for partition-related operations shared across Delta Kernel Spark components. */
public class PartitionUtils {

  /**
   * Calculate the maximum split bytes for file partitioning, considering total bytes and file
   * count. This is used for optimal file splitting in both batch and streaming scenarios.
   *
   * <p>The algorithm balances several factors:
   *
   * <ul>
   *   <li>Ensures good parallelism by dividing work across available cores
   *   <li>Respects the maximum partition size (spark.sql.files.maxPartitionBytes)
   *   <li>Accounts for file open overhead (spark.sql.files.openCostInBytes)
   *   <li>Prevents creating partitions smaller than the open cost
   * </ul>
   *
   * @param sparkSession the Spark session
   * @param totalBytes total bytes across all files
   * @param fileCount number of files
   * @param sqlConf the SQL configuration
   * @return maximum split bytes for partitioning
   */
  public static long calculateMaxSplitBytes(
      SparkSession sparkSession, long totalBytes, int fileCount, SQLConf sqlConf) {
    long defaultMaxSplitBytes = sqlConf.filesMaxPartitionBytes();
    long openCostInBytes = sqlConf.filesOpenCostInBytes();
    Option<Object> minPartitionNumOption = sqlConf.filesMinPartitionNum();

    int minPartitionNum =
        minPartitionNumOption.isDefined()
            ? ((Number) minPartitionNumOption.get()).intValue()
            : sparkSession.leafNodeDefaultParallelism();
    if (minPartitionNum <= 0) {
      minPartitionNum = 1;
    }

    long calculatedTotalBytes = totalBytes + (long) fileCount * openCostInBytes;
    long bytesPerCore = calculatedTotalBytes / minPartitionNum;

    return Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore));
  }

  /**
   * Calculate the maximum split bytes for file partitioning using file list. Convenience method
   * that calculates total bytes from the file list.
   *
   * @param sparkSession the Spark session
   * @param partitionedFiles list of partitioned files
   * @param sqlConf the SQL configuration
   * @return maximum split bytes for partitioning
   */
  public static long calculateMaxSplitBytes(
      SparkSession sparkSession, List<PartitionedFile> partitionedFiles, SQLConf sqlConf) {
    long totalBytes = partitionedFiles.stream().mapToLong(PartitionedFile::fileSize).sum();
    return calculateMaxSplitBytes(sparkSession, totalBytes, partitionedFiles.size(), sqlConf);
  }

  /**
   * Build the partition InternalRow from kernel partition values by casting them to the desired
   * Spark types using the session time zone for temporal types.
   *
   * <p>This method handles the conversion of Delta Kernel's partition values (stored as strings in
   * a MapValue) to Spark's InternalRow representation, properly typed for each partition column.
   *
   * @param partitionValues the partition values from kernel (AddFile.getPartitionValues())
   * @param partitionSchema the partition schema defining the expected types
   * @param zoneId the time zone for temporal type conversions
   * @return InternalRow containing the typed partition values
   */
  public static InternalRow getPartitionRow(
      MapValue partitionValues, StructType partitionSchema, ZoneId zoneId) {
    final int numPartCols = partitionSchema.fields().length;
    final Object[] values = new Object[numPartCols];

    // Build field name -> index map once
    final Map<String, Integer> fieldIndex = new HashMap<>(numPartCols);
    for (int i = 0; i < numPartCols; i++) {
      fieldIndex.put(partitionSchema.fields()[i].name(), i);
      values[i] = null;
    }

    // Fill values in a single pass over partitionValues
    for (int idx = 0; idx < partitionValues.getSize(); idx++) {
      final String key = partitionValues.getKeys().getString(idx);
      final String strVal = partitionValues.getValues().getString(idx);
      final Integer pos = fieldIndex.get(key);
      if (pos != null) {
        final StructField field = partitionSchema.fields()[pos];
        values[pos] =
            (strVal == null)
                ? null
                : PartitioningUtils.castPartValueToDesiredType(field.dataType(), strVal, zoneId);
      }
    }
    return InternalRow.fromSeq(
        JavaConverters.asScalaIterator(Arrays.asList(values).iterator()).toSeq());
  }

  /**
   * Create a PartitionReaderFactory for reading Parquet files with partition support.
   *
   * <p>This method creates a reader factory that uses Spark's ParquetFileFormat to read files, with
   * support for:
   *
   * <ul>
   *   <li>Vectorized reading when schema is compatible
   *   <li>Partition column injection
   *   <li>Data filter pushdown
   *   <li>Schema projection
   * </ul>
   *
   * @param dataSchema the full data schema of the table
   * @param partitionSchema the partition schema
   * @param readDataSchema the projected data schema to read
   * @param dataFilters filters to push down to the file format
   * @param scalaOptions options to pass to the file format
   * @param hadoopConf Hadoop configuration for file access
   * @param sqlConf SQL configuration for vectorization checks
   * @return PartitionReaderFactory for reading the files
   */
  public static PartitionReaderFactory createParquetReaderFactory(
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Filter[] dataFilters,
      scala.collection.immutable.Map<String, String> scalaOptions,
      Configuration hadoopConf,
      SQLConf sqlConf) {
    boolean enableVectorizedReader =
        ParquetUtils.isBatchReadSupportedForSchema(sqlConf, readDataSchema);
    scala.collection.immutable.Map<String, String> optionsWithBatch =
        scalaOptions.$plus(
            new Tuple2<>(
                FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                String.valueOf(enableVectorizedReader)));
    Function1<PartitionedFile, Iterator<InternalRow>> readFunc =
        new ParquetFileFormat()
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
}
