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
package io.delta.spark.dsv2.read;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.utils.CloseableIterator;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.execution.datasources.*;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Option;
import scala.collection.JavaConverters;

/**
 * Spark Scan implementation backed by Delta Kernel.
 *
 * <p>Created on Driver and provides access to batch and streaming scanning capabilities.
 */
public class SparkScan implements Scan, Batch, SupportsReportStatistics {

  private final String tableName;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Predicate[] pushedToKernelFilters;
  private final Filter[] dataFilters;
  private final io.delta.kernel.Scan kernelScan;
  private final Configuration hadoopConf;
  private long totalBytes = 0L;
  private List<PartitionedFile> partitionedFiles = new ArrayList<>();

  private Set<Predicate> allKernelFilters;
  private final SQLConf sqlConf;
  private final ZoneId zoneId;

  public SparkScan(
      String tableName,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Predicate[] pushedToKernelFilters,
      Filter[] dataFilters,
      io.delta.kernel.Scan kernelScan,
      Configuration hadoopConf) {

    this.tableName = tableName;
    this.dataSchema = dataSchema;
    this.partitionSchema = partitionSchema;
    this.readDataSchema = readDataSchema;
    this.pushedToKernelFilters = pushedToKernelFilters;
    this.dataFilters = dataFilters;
    this.kernelScan = kernelScan;
    this.hadoopConf = hadoopConf;

    this.allKernelFilters = new HashSet<>(Arrays.asList(pushedToKernelFilters));
    this.sqlConf = SQLConf.get();
    this.zoneId = ZoneId.of(sqlConf.sessionLocalTimeZone());
    // Plan partitions and compute total size
    planPartitions();
  }

  @Override
  public StructType readSchema() {
    List<StructField> fields = new ArrayList<>();
    fields.addAll(Arrays.asList(readDataSchema.fields()));
    fields.addAll(Arrays.asList(partitionSchema.fields()));
    return new StructType(fields.toArray(new StructField[0]));
  }

  private InternalRow getPartitionRow(MapValue partitionValues) {
    assert partitionValues.getSize() == partitionSchema.fields().length
        : String.format(
            "Partition values size from add file %d != partition columns size %d",
            partitionValues.getSize(), partitionSchema.fields().length);

    Map<String, String> partitionValueMap = new HashMap<>();
    for (int idx = 0; idx < partitionValues.getSize(); idx++) {
      String key = partitionValues.getKeys().getString(idx);
      String value = partitionValues.getValues().getString(idx);
      partitionValueMap.put(key, value);
    }

    Object[] values = new Object[partitionSchema.fields().length];
    for (int i = 0; i < partitionSchema.fields().length; i++) {
      StructField field = partitionSchema.fields()[i];
      String value = partitionValueMap.get(field.name());
      if (value == null) {
        values[i] = null;
      } else {
        values[i] = PartitioningUtils.castPartValueToDesiredType(field.dataType(), value, zoneId);
      }
    }
    return InternalRow.fromSeq(
        JavaConverters.asScalaIterator(Arrays.asList(values).iterator()).toSeq());
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public String description() {
    return String.format(
        "PushedFilters: [%s], DataFilters: [%s]",
        Arrays.stream(pushedToKernelFilters)
            .map(Object::toString)
            .collect(Collectors.joining(", ")),
        Arrays.stream(dataFilters).map(Object::toString).collect(Collectors.joining(", ")));
  }

  @Override
  public Statistics estimateStatistics() {
    return new Statistics() {
      @Override
      public OptionalLong sizeInBytes() {
        return OptionalLong.of(totalBytes);
      }

      @Override
      public OptionalLong numRows() {
        return OptionalLong.empty();
      }
    };
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof SparkScan)) return false;

    SparkScan that = (SparkScan) obj;
    return Objects.equals(this.tableName, that.tableName)
        && Objects.equals(this.readSchema(), that.readSchema())
        && Arrays.equals(this.pushedToKernelFilters, that.pushedToKernelFilters)
        && Arrays.equals(this.dataFilters, that.dataFilters);
  }

  @Override
  public int hashCode() {
    int result = tableName.hashCode();
    result = 31 * result + readSchema().hashCode();
    result = 31 * result + Arrays.hashCode(pushedToKernelFilters);
    result = 31 * result + Arrays.hashCode(dataFilters);
    return result;
  }

  private Long calculateMaxSplitBytes(SparkSession sparkSession) {
    long defaultMaxSplitBytes = sqlConf.filesMaxPartitionBytes();
    long openCostInBytes = sqlConf.filesOpenCostInBytes();
    Option<Object> minPartitionNumOption = sqlConf.filesMinPartitionNum();

    int minPartitionNum =
        minPartitionNumOption.isDefined()
            ? (Integer) minPartitionNumOption.get()
            : sparkSession.leafNodeDefaultParallelism();
    long calculatedTotalBytes = totalBytes + partitionedFiles.size() * openCostInBytes;
    long bytesPerCore = calculatedTotalBytes / minPartitionNum;

    return Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore));
  }

  @Override
  public InputPartition[] planInputPartitions() {
    SparkSession sparkSession = SparkSession.active();
    Long maxSplitBytes = calculateMaxSplitBytes(sparkSession);

    scala.collection.Seq<FilePartition> filePartitions =
        FilePartition$.MODULE$.getFilePartitions(
            sparkSession, JavaConverters.asScalaBuffer(partitionedFiles).toSeq(), maxSplitBytes);
    return JavaConverters.seqAsJavaList(filePartitions).toArray(new InputPartition[0]);
  }

  private void planPartitions() {
    Engine tableEngine = DefaultEngine.create(hadoopConf);
    Iterator<io.delta.kernel.data.FilteredColumnarBatch> addFiles =
        kernelScan.getScanFiles(tableEngine);

    String[] locations = new String[0];
    scala.collection.immutable.Map<String, Object> otherConstantMetadataColumnValues =
        scala.collection.immutable.Map$.MODULE$.empty();

    while (addFiles.hasNext()) {
      CloseableIterator<Row> addFileRowIter = addFiles.next().getRows();
      while (addFileRowIter.hasNext()) {
        Row parquetRow = addFileRowIter.next();
        AddFile addFile = new AddFile(parquetRow.getStruct(0));

        PartitionedFile partitionedFile =
            new PartitionedFile(
                getPartitionRow(addFile.getPartitionValues()),
                SparkPath.fromUrlString(addFile.getPath()),
                0L,
                addFile.getSize(),
                locations,
                addFile.getModificationTime(),
                addFile.getSize(),
                otherConstantMetadataColumnValues);

        totalBytes += addFile.getSize();
        partitionedFiles.add(partitionedFile);
      }
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    boolean enableVectorizedReader =
        ParquetUtils.isBatchReadSupportedForSchema(sqlConf, readSchema());

    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$
            .<String, String>empty()
            .updated(
                FileFormat$.MODULE$.OPTION_RETURNING_BATCH(),
                String.valueOf(enableVectorizedReader));
    Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> readFunc =
        new ParquetFileFormat()
            .buildReaderWithPartitionValues(
                SparkSession.active(),
                dataSchema,
                partitionSchema,
                readDataSchema,
                JavaConverters.asScalaBuffer(Arrays.asList(dataFilters)).toSeq(),
                options,
                hadoopConf);

    return new SparkReaderFactory(readFunc, enableVectorizedReader);
  }
}
