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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;

/**
 * Kernel {@link Scan} implementation backed by a pre-filtered list of Spark V1 {@link
 * org.apache.spark.sql.delta.actions.AddFile} objects.
 *
 * <p>{@link DistributedScanBuilder} orchestrates the full V1 shared pipeline (state reconstruction
 * + filter planning + scan execution) and passes the surviving files to this class. This class only
 * handles the Spark-to-Kernel row conversion boundary — no filtering is performed here.
 *
 * <p>{@code SparkScan} consumes the output of {@link #getScanFiles(Engine)} without changes,
 * because the returned rows conform to the standard Kernel scan-file schema.
 *
 * @see DistributedScanBuilder builder that creates this scan
 */
public final class DistributedScan implements Scan {

  private final List<org.apache.spark.sql.delta.actions.AddFile> filteredFiles;
  private final String tableRoot;
  private final Scan delegateScan; // for getScanState / getRemainingFilter

  /**
   * Creates a DistributedScan from pre-filtered V1 AddFile objects.
   *
   * @param filteredFiles the files that survived partition pruning + data skipping
   * @param tableRoot the table root path (with trailing slash)
   * @param snapshot kernel snapshot for delegation metadata
   * @param readSchema read schema for Kernel delegation
   */
  DistributedScan(
      List<org.apache.spark.sql.delta.actions.AddFile> filteredFiles,
      String tableRoot,
      Snapshot snapshot,
      StructType readSchema) {
    this.filteredFiles = requireNonNull(filteredFiles, "filteredFiles");
    this.tableRoot = requireNonNull(tableRoot, "tableRoot");
    requireNonNull(snapshot, "snapshot");
    requireNonNull(readSchema, "readSchema");
    this.delegateScan = snapshot.getScanBuilder().withReadSchema(readSchema).build();
  }

  /**
   * Returns the filtered files as Kernel {@link FilteredColumnarBatch} objects.
   *
   * <p>Each batch contains one file row conforming to {@link
   * InternalScanFileUtils#SCAN_FILE_SCHEMA}. The rows are produced by converting Spark V1 {@link
   * org.apache.spark.sql.delta.actions.AddFile} to Kernel {@link GenericRow}.
   */
  @Override
  public CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine) {
    // Convert all Spark AddFiles to Kernel scan file rows in one batch
    List<Row> scanFileRows =
        filteredFiles.stream().map(this::toKernelScanFileRow).collect(Collectors.toList());

    // Wrap as a single FilteredColumnarBatch using DefaultRowBasedColumnarBatch
    io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch batch =
        new io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch(
            InternalScanFileUtils.SCAN_FILE_SCHEMA, scanFileRows);

    FilteredColumnarBatch filteredBatch = new FilteredColumnarBatch(batch, Optional.empty());

    // Return single-element iterator
    return new CloseableIterator<FilteredColumnarBatch>() {
      private boolean hasNext = !scanFileRows.isEmpty();

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public FilteredColumnarBatch next() {
        if (!hasNext) {
          throw new NoSuchElementException();
        }
        hasNext = false;
        return filteredBatch;
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public Optional<Predicate> getRemainingFilter() {
    return delegateScan.getRemainingFilter();
  }

  @Override
  public Row getScanState(Engine engine) {
    return delegateScan.getScanState(engine);
  }

  // ========================
  // Spark V1 -> Kernel Row conversion
  // ========================

  /**
   * Converts a Spark V1 AddFile to a Kernel scan file row conforming to {@link
   * InternalScanFileUtils#SCAN_FILE_SCHEMA}.
   *
   * <p>The scan file row schema is: {add: AddFile struct, tableRoot: String}
   */
  private Row toKernelScanFileRow(org.apache.spark.sql.delta.actions.AddFile sparkAddFile) {
    Row addFileRow = toKernelAddFileRow(sparkAddFile);

    Map<Integer, Object> scanFileMap = new HashMap<>();
    scanFileMap.put(InternalScanFileUtils.SCAN_FILE_SCHEMA.indexOf("add"), addFileRow);
    scanFileMap.put(InternalScanFileUtils.SCAN_FILE_SCHEMA.indexOf("tableRoot"), tableRoot);

    return new GenericRow(InternalScanFileUtils.SCAN_FILE_SCHEMA, scanFileMap);
  }

  /**
   * Converts a Spark V1 AddFile to a Kernel AddFile row conforming to {@link
   * AddFile#SCHEMA_WITHOUT_STATS}.
   */
  private Row toKernelAddFileRow(org.apache.spark.sql.delta.actions.AddFile sparkAddFile) {
    StructType schema = AddFile.SCHEMA_WITHOUT_STATS;
    Map<Integer, Object> fieldMap = new HashMap<>();

    // Required fields
    fieldMap.put(schema.indexOf("path"), sparkAddFile.path());
    fieldMap.put(schema.indexOf("size"), sparkAddFile.size());
    fieldMap.put(schema.indexOf("modificationTime"), sparkAddFile.modificationTime());
    fieldMap.put(schema.indexOf("dataChange"), sparkAddFile.dataChange());

    // partitionValues: Scala Map -> Kernel MapValue
    Map<String, String> partitionValues = new HashMap<>();
    scala.collection.immutable.Map<String, String> scalaMap = sparkAddFile.partitionValues();
    scala.collection.Iterator<scala.Tuple2<String, String>> iter = scalaMap.iterator();
    while (iter.hasNext()) {
      scala.Tuple2<String, String> entry = iter.next();
      partitionValues.put(entry._1(), entry._2());
    }
    fieldMap.put(
        schema.indexOf("partitionValues"), VectorUtils.stringStringMapValue(partitionValues));

    // deletionVector (nullable)
    DeletionVectorDescriptor dv = sparkAddFile.deletionVector();
    if (dv != null) {
      fieldMap.put(schema.indexOf("deletionVector"), toKernelDeletionVectorRow(dv));
    }

    // tags (nullable Map, not Option)
    scala.collection.immutable.Map<String, String> tagMap = sparkAddFile.tags();
    if (tagMap != null && !tagMap.isEmpty()) {
      Map<String, String> tags = new HashMap<>();
      scala.collection.Iterator<scala.Tuple2<String, String>> tagsIter = tagMap.iterator();
      while (tagsIter.hasNext()) {
        scala.Tuple2<String, String> entry = tagsIter.next();
        tags.put(entry._1(), entry._2());
      }
      fieldMap.put(schema.indexOf("tags"), VectorUtils.stringStringMapValue(tags));
    }

    // baseRowId (nullable)
    scala.Option<Object> baseRowIdOpt = sparkAddFile.baseRowId();
    if (baseRowIdOpt.isDefined()) {
      fieldMap.put(schema.indexOf("baseRowId"), (long) baseRowIdOpt.get());
    }

    // defaultRowCommitVersion (nullable)
    scala.Option<Object> defaultRowCommitVersionOpt = sparkAddFile.defaultRowCommitVersion();
    if (defaultRowCommitVersionOpt.isDefined()) {
      fieldMap.put(
          schema.indexOf("defaultRowCommitVersion"), (long) defaultRowCommitVersionOpt.get());
    }

    return new GenericRow(schema, fieldMap);
  }

  /**
   * Converts a Spark V1 DeletionVectorDescriptor to a Kernel DeletionVector row conforming to
   * {@link io.delta.kernel.internal.actions.DeletionVectorDescriptor#READ_SCHEMA}.
   */
  private Row toKernelDeletionVectorRow(DeletionVectorDescriptor dv) {
    StructType dvSchema = io.delta.kernel.internal.actions.DeletionVectorDescriptor.READ_SCHEMA;
    Map<Integer, Object> dvMap = new HashMap<>();

    dvMap.put(dvSchema.indexOf("storageType"), dv.storageType());
    dvMap.put(dvSchema.indexOf("pathOrInlineDv"), dv.pathOrInlineDv());
    dvMap.put(dvSchema.indexOf("offset"), (int) dv.offset().getOrElse(() -> 0));
    dvMap.put(dvSchema.indexOf("sizeInBytes"), dv.sizeInBytes());
    dvMap.put(dvSchema.indexOf("cardinality"), dv.cardinality());

    return new GenericRow(dvSchema, dvMap);
  }
}
