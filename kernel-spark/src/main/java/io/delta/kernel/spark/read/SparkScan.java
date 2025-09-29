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

import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.spark.utils.ScalaUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.execution.datasources.*;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;

/** Spark DSV2 Scan implementation backed by Delta Kernel. */
public class SparkScan implements Scan, SupportsReportStatistics, SupportsRuntimeV2Filtering {

  private final String tablePath;
  private final StructType readDataSchema;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final Predicate[] pushedToKernelFilters;
  private final Filter[] dataFilters;
  private final io.delta.kernel.Scan kernelScan;
  private final Configuration hadoopConf;
  private final CaseInsensitiveStringMap options;
  private final scala.collection.immutable.Map<String, String> scalaOptions;
  private final SQLConf sqlConf;
  private final ZoneId zoneId;

  // Planned input files and stats
  private final List<PartitionedFile> partitionedFiles = new ArrayList<>();
  private long totalBytes = 0L;
  private volatile boolean planned = false;

  public SparkScan(
      String tablePath,
      StructType dataSchema,
      StructType partitionSchema,
      StructType readDataSchema,
      Predicate[] pushedToKernelFilters,
      Filter[] dataFilters,
      io.delta.kernel.Scan kernelScan,
      CaseInsensitiveStringMap options) {

    final String normalizedTablePath = Objects.requireNonNull(tablePath, "tablePath is null");
    this.tablePath =
        normalizedTablePath.endsWith("/") ? normalizedTablePath : normalizedTablePath + "/";
    this.dataSchema = Objects.requireNonNull(dataSchema, "dataSchema is null");
    this.partitionSchema = Objects.requireNonNull(partitionSchema, "partitionSchema is null");
    this.readDataSchema = Objects.requireNonNull(readDataSchema, "readDataSchema is null");
    this.pushedToKernelFilters =
        pushedToKernelFilters == null ? new Predicate[0] : pushedToKernelFilters.clone();
    this.dataFilters = dataFilters == null ? new Filter[0] : dataFilters.clone();
    this.kernelScan = Objects.requireNonNull(kernelScan, "kernelScan is null");
    this.options = Objects.requireNonNull(options, "options is null");
    this.scalaOptions = ScalaUtils.toScalaMap(options);
    this.hadoopConf = SparkSession.active().sessionState().newHadoopConfWithOptions(scalaOptions);
    this.sqlConf = SQLConf.get();
    this.zoneId = ZoneId.of(sqlConf.sessionLocalTimeZone());
  }

  /**
   * Read schema for the scan, which is the projection of data columns followed by partition
   * columns.
   */
  @Override
  public StructType readSchema() {
    final List<StructField> fields =
        new ArrayList<>(readDataSchema.fields().length + partitionSchema.fields().length);
    Collections.addAll(fields, readDataSchema.fields());
    Collections.addAll(fields, partitionSchema.fields());
    return new StructType(fields.toArray(new StructField[0]));
  }

  @Override
  public Batch toBatch() {
    ensurePlanned();
    return new SparkBatch(
        tablePath,
        dataSchema,
        partitionSchema,
        readDataSchema,
        partitionedFiles,
        pushedToKernelFilters,
        dataFilters,
        totalBytes,
        scalaOptions,
        hadoopConf);
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return new SparkMicroBatchStream();
  }

  @Override
  public String description() {
    final String pushed =
        Arrays.stream(pushedToKernelFilters)
            .map(Object::toString)
            .collect(Collectors.joining(", "));
    final String data =
        Arrays.stream(dataFilters).map(Object::toString).collect(Collectors.joining(", "));
    return String.format(Locale.ROOT, "PushedFilters: [%s], DataFilters: [%s]", pushed, data);
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
        // Row count is unknown at planning time.
        return OptionalLong.empty();
      }
    };
  }

  /**
   * Build the partition {@link InternalRow} from kernel partition values by casting them to the
   * desired Spark types using the session time zone for temporal types.
   */
  private InternalRow getPartitionRow(MapValue partitionValues) {
    final int numPartCols = partitionSchema.fields().length;
    assert partitionValues.getSize() == numPartCols
        : String.format(
            Locale.ROOT,
            "Partition values size from add file %d != partition columns size %d",
            partitionValues.getSize(),
            numPartCols);

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
   * Plan the files to scan by materializing {@link PartitionedFile}s and aggregating size stats.
   * Ensures all iterators are closed to avoid resource leaks.
   */
  private void planScanFiles() {
    final Engine tableEngine = DefaultEngine.create(hadoopConf);
    final Iterator<io.delta.kernel.data.FilteredColumnarBatch> scanFileBatches =
        kernelScan.getScanFiles(tableEngine);

    final String[] locations = new String[0];
    final scala.collection.immutable.Map<String, Object> otherConstantMetadataColumnValues =
        scala.collection.immutable.Map$.MODULE$.empty();

    while (scanFileBatches.hasNext()) {
      final io.delta.kernel.data.FilteredColumnarBatch batch = scanFileBatches.next();

      try (CloseableIterator<Row> addFileRowIter = batch.getRows()) {
        while (addFileRowIter.hasNext()) {
          final Row row = addFileRowIter.next();
          final AddFile addFile = new AddFile(row.getStruct(0));

          final PartitionedFile partitionedFile =
              new PartitionedFile(
                  getPartitionRow(addFile.getPartitionValues()),
                  SparkPath.fromUrlString(tablePath + addFile.getPath()),
                  0L,
                  addFile.getSize(),
                  locations,
                  addFile.getModificationTime(),
                  addFile.getSize(),
                  otherConstantMetadataColumnValues);

          totalBytes += addFile.getSize();
          partitionedFiles.add(partitionedFile);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Ensure the scan is planned exactly once in a thread\-safe manner. */
  private synchronized void ensurePlanned() {
    if (!planned) {
      planScanFiles();
      planned = true;
    }
  }

  StructType getDataSchema() {
    return dataSchema;
  }

  StructType getPartitionSchema() {
    return partitionSchema;
  }

  StructType getReadDataSchema() {
    return readDataSchema;
  }

  CaseInsensitiveStringMap getOptions() {
    return options;
  }

  Configuration getConfiguration() {
    return hadoopConf;
  }

  @Override
  public NamedReference[] filterAttributes() {
    return Arrays.stream(partitionSchema.fields())
        .map(field -> FieldReference.column(field.name()))
        .toArray(NamedReference[]::new);
  }

  @Override
  public void filter(org.apache.spark.sql.connector.expressions.filter.Predicate[] predicates) {
    // 1. get all predicates on partition columns
    final Set<String> partitionColNames =
        Arrays.stream(partitionSchema.fields()).map(StructField::name).collect(Collectors.toSet());
    List<org.apache.spark.sql.connector.expressions.filter.Predicate> partitionPredicates =
        new ArrayList<>();
    for (org.apache.spark.sql.connector.expressions.filter.Predicate predicate : predicates) {
      boolean isPartitionPredicate = true;
      for (NamedReference ref : predicate.references()) {
        if (!partitionColNames.contains(ref.fieldNames()[0])) {
          isPartitionPredicate = false;
          break;
        }
      }
      if (isPartitionPredicate) {
        partitionPredicates.add(predicate);
      }
    }

    // 2. apply partition predicates to filter partitionedFiles
    ensurePlanned();
    List<PartitionedFile> runtimeFilteredPartitionedFiles = new ArrayList<>();
    for (PartitionedFile pf : partitionedFiles) {
      boolean keep = true;
      for (org.apache.spark.sql.connector.expressions.filter.Predicate predicate :
          partitionPredicates) {
        if (!evaluatePartitionValueOnPredicate(predicate, pf.partitionValues())) {
          keep = false;
          break;
        }
      }
      if (keep) {
        runtimeFilteredPartitionedFiles.add(pf);
      }
    }

    // 3. update totalBytes and partitionedFiles
    this.partitionedFiles.clear();
    this.partitionedFiles.addAll(runtimeFilteredPartitionedFiles);
    this.totalBytes = 0L;
    for (PartitionedFile pf : this.partitionedFiles) {
      this.totalBytes += pf.fileSize();
    }
  }

  /*
   * Evaluate a predicate on partition values.
   * Return true if the partition values satisfy the predicate, false otherwise.
   */
  private boolean evaluatePartitionValueOnPredicate(
      org.apache.spark.sql.connector.expressions.filter.Predicate predicate,
      InternalRow partitionValues) {
    try {
      // Convert the DSV2 predicate to Catalyst expression
      Expression catalystExpr = dsv2PredicateToCatalystExpression(predicate, partitionSchema);

      // Use an interpreted predicate for evaluation
      BasePredicate basePredicate = new InterpretedPredicate(catalystExpr);

      // Evaluate and return result
      return basePredicate.eval(partitionValues);

    } catch (Exception e) {
      // Log the error and return true (conservative approach - don't filter out)
      return true;
    }
  }

  private Expression dsv2PredicateToCatalystExpression(
      org.apache.spark.sql.connector.expressions.filter.Predicate predicate, StructType schema) {

    String predicateName = predicate.name();
    org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();

    switch (predicateName) {
      case "=":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.EqualTo(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case ">":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.GreaterThan(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case ">=":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "<":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.LessThan(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "<=":
        if (children.length == 2) {
          return new org.apache.spark.sql.catalyst.expressions.LessThanOrEqual(
              resolveExpression(children[0], schema), resolveExpression(children[1], schema));
        }
        break;

      case "IS_NULL":
        if (children.length == 1) {
          return new org.apache.spark.sql.catalyst.expressions.IsNull(
              resolveExpression(children[0], schema));
        }
        break;

      case "IS_NOT_NULL":
        if (children.length == 1) {
          return new org.apache.spark.sql.catalyst.expressions.IsNotNull(
              resolveExpression(children[0], schema));
        }
        break;

      case "AND":
        if (predicate instanceof org.apache.spark.sql.connector.expressions.filter.And) {
          org.apache.spark.sql.connector.expressions.filter.And and =
              (org.apache.spark.sql.connector.expressions.filter.And) predicate;
          return new org.apache.spark.sql.catalyst.expressions.And(
              dsv2PredicateToCatalystExpression(and.left(), schema),
              dsv2PredicateToCatalystExpression(and.right(), schema));
        }
        break;

      case "OR":
        if (predicate instanceof org.apache.spark.sql.connector.expressions.filter.Or) {
          org.apache.spark.sql.connector.expressions.filter.Or or =
              (org.apache.spark.sql.connector.expressions.filter.Or) predicate;
          return new org.apache.spark.sql.catalyst.expressions.Or(
              dsv2PredicateToCatalystExpression(or.left(), schema),
              dsv2PredicateToCatalystExpression(or.right(), schema));
        }
        break;

      case "NOT":
        if (predicate instanceof org.apache.spark.sql.connector.expressions.filter.Not) {
          org.apache.spark.sql.connector.expressions.filter.Not not =
              (org.apache.spark.sql.connector.expressions.filter.Not) predicate;
          return new org.apache.spark.sql.catalyst.expressions.Not(
              dsv2PredicateToCatalystExpression(not.child(), schema));
        }
        break;

      case "IN":
        if (children.length >= 2) {
          List<Expression> values = new ArrayList<>();
          for (int i = 1; i < children.length; i++) {
            values.add(resolveExpression(children[i], schema));
          }
          return new org.apache.spark.sql.catalyst.expressions.In(
              resolveExpression(children[0], schema), JavaConverters.asScalaBuffer(values).toSeq());
        }
        break;
    }

    // Default to always true for unsupported predicates
    return org.apache.spark.sql.catalyst.expressions.Literal.create(
        true, org.apache.spark.sql.types.DataTypes.BooleanType);
  }

  private Expression resolveExpression(
      org.apache.spark.sql.connector.expressions.Expression expr, StructType schema) {
    if (expr instanceof NamedReference) {
      NamedReference ref = (NamedReference) expr;
      String columnName = ref.fieldNames()[0];
      int index = java.util.Arrays.asList(schema.fieldNames()).indexOf(columnName);
      if (index >= 0) {
        StructField field = schema.fields()[index];
        return new BoundReference(index, field.dataType(), field.nullable());
      }
      throw new IllegalArgumentException("Column not found: " + columnName);
    } else if (expr instanceof LiteralValue) {
      LiteralValue<?> literal = (LiteralValue<?>) expr;
      return org.apache.spark.sql.catalyst.expressions.Literal.create(
          literal.value(), literal.dataType());
    } else {
      // For unsupported expression types, return a literal true
      return org.apache.spark.sql.catalyst.expressions.Literal.create(
          true, org.apache.spark.sql.types.DataTypes.BooleanType);
    }
  }
}
