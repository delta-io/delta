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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.execution.datasources.*;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

/** Spark DSV2 Scan implementation backed by Delta Kernel. */
public class SparkScan implements Scan, SupportsReportStatistics {

  private final String tableName;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Predicate[] pushedToKernelFilters;
  private final Filter[] dataFilters;
  private final io.delta.kernel.Scan kernelScan;
  private final Configuration hadoopConf;
  private final SQLConf sqlConf;
  private final ZoneId zoneId;
  private List<PartitionedFile> partitionedFiles = new ArrayList<>();
  private long totalBytes = 0L;
  private volatile boolean planned = false;

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
    this.sqlConf = SQLConf.get();
    this.zoneId = ZoneId.of(sqlConf.sessionLocalTimeZone());
  }

  @Override
  public StructType readSchema() {
    List<StructField> fields = new ArrayList<>();
    fields.addAll(Arrays.asList(readDataSchema.fields()));
    fields.addAll(Arrays.asList(partitionSchema.fields()));
    return new StructType(fields.toArray(new StructField[0]));
  }

  @Override
  public Batch toBatch() {
    return new SparkBatch(
        tableName,
        dataSchema,
        partitionSchema,
        readDataSchema,
        partitionedFiles,
        pushedToKernelFilters,
        dataFilters,
        totalBytes,
        hadoopConf);
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
    ensurePlanned();
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

  private void getScanFiles() {
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

  private synchronized void ensurePlanned() {
    if (!planned) {
      getScanFiles();
      planned = true;
    }
  }
}
