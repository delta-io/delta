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
package org.apache.spark.sql.delta.v2.read;

import io.delta.kernel.exceptions.UnsupportedProtocolVersionException;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.*;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.IndexedFile;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.delta.storage.ClosableIterator.IteratorCloseOps;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction2;

/** Static helpers and constants of {@link DeltaV2MicroBatchStream}. */
final class DeltaV2MicroBatchStreamUtils {

  private DeltaV2MicroBatchStreamUtils() {}

  /**
   * If the given exception wraps a {@link ClosedByInterruptException} as its direct cause, returns
   * it. This occurs when Spark interrupts the micro-batch thread during stream shutdown and the
   * thread is blocked inside Kernel's {@code DefaultJsonHandler} reading delta log files via NIO
   * channels.
   */
  static Optional<ClosedByInterruptException> findClosedByInterruptCause(Throwable t) {
    Throwable cause = t.getCause();
    if (cause instanceof ClosedByInterruptException) {
      return Optional.of((ClosedByInterruptException) cause);
    }
    return Optional.empty();
  }

  /**
   * Walks the cause chain of the given throwable looking for a Kernel {@link
   * UnsupportedProtocolVersionException}.
   */
  static Optional<UnsupportedProtocolVersionException> findUnsupportedProtocolCause(Throwable t) {
    for (Throwable current = t; current != null; current = current.getCause()) {
      if (current instanceof UnsupportedProtocolVersionException) {
        return Optional.of((UnsupportedProtocolVersionException) current);
      }
      if (current.getCause() == current) {
        break;
      }
    }
    return Optional.empty();
  }

  /**
   * Returns true if the file is strictly after the (exclusive) resume point (fromVersion,
   * fromIndex).
   */
  static boolean isAfterStartBoundary(IndexedFile file, long fromVersion, long fromIndex) {
    return file.version() > fromVersion
        || (file.version() == fromVersion && file.index() > fromIndex);
  }

  /**
   * Validates the partition schema and physical names without comparing unrelated metadata.
   *
   * <p>Partition changes need a separate check because full-schema validation sorts the columns and
   * does not distinguish partition columns from data columns.
   */
  static void validatePartitionSchemaOnStartup(
      StructType partitionSchema, StructType readPartitionSchemaAtSourceInit, String tableId) {
    // Physical names identify the partition values in each file.
    // Partition reads tolerate reordering, but keep this check consistent with the order-sensitive
    // partition-change checks.
    boolean namesMatch =
        Arrays.equals(partitionSchema.fieldNames(), readPartitionSchemaAtSourceInit.fieldNames());
    boolean typesMatch =
        DataType.equalsStructurally(
            partitionSchema, readPartitionSchemaAtSourceInit, /* ignoreNullability= */ false);
    boolean physicalNamesMatch =
        Arrays.equals(
            Arrays.stream(partitionSchema.fields())
                .map(DeltaColumnMapping::getPhysicalName)
                .toArray(String[]::new),
            Arrays.stream(readPartitionSchemaAtSourceInit.fields())
                .map(DeltaColumnMapping::getPhysicalName)
                .toArray(String[]::new));
    if (!namesMatch || !typesMatch || !physicalNamesMatch) {
      throw DeltaErrors.streamingPartitionSchemaMismatchOnRestart(
          tableId, partitionSchema, readPartitionSchemaAtSourceInit);
    }
  }

  /**
   * Validates that the analysis-time schema matches the latest snapshot schema. This catches cases
   * where the table schema changed after the streaming query was analyzed, and the user restarts
   * the stream with a stale DataFrame.
   *
   * @param dataSchema data columns from analysis time (excludes partition columns)
   * @param partitionSchema partition columns from analysis time
   * @param snapshotSchema full table schema from the latest snapshot at stream start
   */
  static void validateSchemaCompatibilityOnStartup(
      StructType dataSchema, StructType partitionSchema, StructType snapshotSchema) {
    // Reconstruct the full analysis-time table schema from dataSchema + partitionSchema.
    // StructType is immutable — add() returns a new instance without modifying the original.
    StructType querySchema = dataSchema;
    for (StructField field : partitionSchema.fields()) {
      querySchema = querySchema.add(field);
    }
    // Sort both sides so a partition column declared in the middle of the table doesn't
    // trip the check.
    StructType sortedQuery = sortFieldsByName(querySchema);
    StructType sortedSnapshot = sortFieldsByName(snapshotSchema);
    // equalsStructurally checks types + nullability but ignores field names;
    // equalsStructurallyByName checks names but ignores types/nullability.
    boolean typesAndNullabilityMatch =
        DataType.equalsStructurally(sortedQuery, sortedSnapshot, /* ignoreNullability= */ false);
    boolean namesMatch =
        DataType.equalsStructurallyByName(sortedQuery, sortedSnapshot, CASE_INSENSITIVE_RESOLVER);
    if (!typesAndNullabilityMatch || !namesMatch) {
      throw DeltaErrors.streamingSchemaMismatchOnRestart(querySchema, snapshotSchema);
    }
  }

  private static final Function2<String, String, Object> CASE_INSENSITIVE_RESOLVER =
      new AbstractFunction2<String, String, Object>() {
        @Override
        public Object apply(String a, String b) {
          return a.equalsIgnoreCase(b);
        }
      };

  private static StructType sortFieldsByName(StructType schema) {
    StructField[] fields = Arrays.copyOf(schema.fields(), schema.fields().length);
    Arrays.sort(fields, Comparator.comparing(StructField::name));
    StructType result = new StructType();
    for (StructField f : fields) {
      result = result.add(f);
    }
    return result;
  }

  /** Closes the current commit's files before the owning commit iterator. */
  static <T> ClosableIterator<T> wrapIteratorWithCommitIteratorClose(
      ClosableIterator<T> inner, ClosableIterator<?> commits) {
    return new ClosableIterator<T>() {
      private boolean innerClosed;

      @Override
      public boolean hasNext() {
        return inner.hasNext();
      }

      @Override
      public T next() {
        return inner.next();
      }

      @Override
      public void close() throws IOException {
        try (commits) {
          if (!innerClosed) {
            innerClosed = true;
            inner.close();
          }
        }
      }
    };
  }

  /**
   * Creates an IndexedFile with no file action for offset tracking, including metadata change
   * barriers.
   */
  static IndexedFile sentinel(long version, long index) {
    return new IndexedFile(
        version,
        index,
        /* add= */ null,
        /* remove= */ null,
        /* cdc= */ null,
        /* shouldSkip= */ false);
  }

  /**
   * Adds dummy BEGIN_INDEX and END_INDEX IndexedFiles for {@code version} before and after the
   * contents of the iterator. The contents of the iterator must be the IndexedFiles that correspond
   * to this version.
   */
  static ClosableIterator<IndexedFile> addBeginAndEndIndexOffsetsForVersion(
      long version, ClosableIterator<IndexedFile> files) {
    return new IteratorCloseOps<>(files)
        .withClose(
            iterator ->
                Iterator.single(sentinel(version, DeltaSourceOffset.BASE_INDEX()))
                    .$plus$plus(() -> iterator)
                    .$plus$plus(
                        () -> Iterator.single(sentinel(version, DeltaSourceOffset.END_INDEX()))));
  }

  /** Pairs a CDC payload with its index. */
  static Tuple2<IndexedFile, CDCDataFile> indexCDCFile(
      long version, long index, CDCDataFile cdcFile) {
    IndexedFile file =
        new IndexedFile(
            version,
            index,
            cdcFile.getAddFile(),
            cdcFile.getRemoveFile(),
            cdcFile.getAddCDCFile(),
            /* shouldSkip= */ false);
    return new Tuple2<>(file, cdcFile);
  }

  /** Checks if the endOffset is at BASE_INDEX for the given version (early exit condition). */
  static boolean isEndOffsetAtBaseIndex(Optional<DeltaSourceOffset> endOffsetOpt, long version) {
    return endOffsetOpt.isPresent()
        && endOffsetOpt.get().reservoirVersion() == version
        && endOffsetOpt.get().index() == DeltaSourceOffset.BASE_INDEX();
  }
}
