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

import com.google.common.collect.ImmutableSet;
import io.delta.kernel.exceptions.UnsupportedProtocolVersionException;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import java.nio.channels.ClosedByInterruptException;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.actions.Protocol;
import org.apache.spark.sql.delta.sources.AdmittableFile;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function2;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractFunction2;

/** Static helpers and constants of {@link DeltaV2MicroBatchStream}. */
final class DeltaV2MicroBatchStreamUtils {

  private DeltaV2MicroBatchStreamUtils() {}

  public static final Set<DeltaAction> ACTION_SET =
      ImmutableSet.of(
          DeltaAction.ADD,
          DeltaAction.REMOVE,
          DeltaAction.METADATA,
          DeltaAction.PROTOCOL,
          DeltaAction.COMMITINFO);

  /** Action set for CDC reads: {@link #ACTION_SET} plus {@link DeltaAction#CDC}. */
  public static final Set<DeltaAction> CDC_ACTION_SET =
      ImmutableSet.<DeltaAction>builder().addAll(ACTION_SET).add(DeltaAction.CDC).build();

  /**
   * Max attempts to resolve the start version when failOnDataLoss=false. Attempt 1 is the initial
   * try with the user's startVersion (surfaces the StartVersionNotFoundException that triggers the
   * failOnDataLoss check). Attempt 2 retries with the earliest available version to recover when
   * failOnDataLoss=false. Attempt 3 guards against a concurrent log-retention prune in the tiny
   * window between attempt 2's exception and its retry.
   */
  static final int FAIL_ON_DATA_LOSS_FALSE_MAX_ATTEMPTS = 3;

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
    return file.getVersion() > fromVersion
        || (file.getVersion() == fromVersion && file.getIndex() > fromIndex);
  }

  /** Result of commit validation, including skip decision and detected metadata/protocol. */
  static class CommitValidationResult {
    final boolean shouldSkipCommit;
    @Nullable final Metadata metadataAction;
    @Nullable final Protocol protocolAction;

    CommitValidationResult(
        boolean shouldSkipCommit,
        @Nullable Metadata metadataAction,
        @Nullable Protocol protocolAction) {
      this.shouldSkipCommit = shouldSkipCommit;
      this.metadataAction = metadataAction;
      this.protocolAction = protocolAction;
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

  /**
   * Adds dummy BEGIN_INDEX and END_INDEX IndexedFiles for {@code version} before and after the
   * contents of the iterator. The contents of the iterator must be the IndexedFiles that correspond
   * to this version.
   */
  static CloseableIterator<IndexedFile> addBeginAndEndIndexOffsetsForVersion(
      long version, CloseableIterator<IndexedFile> files) {
    return Utils.singletonCloseableIterator(
            IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX()))
        .combine(files)
        .combine(
            Utils.singletonCloseableIterator(
                IndexedFile.sentinel(version, DeltaSourceOffset.END_INDEX())));
  }

  /** Converts a list of IndexedFiles to a Scala Seq of AdmittableFile for batch admission. */
  static Seq<AdmittableFile> toScalaAdmittableSeq(List<IndexedFile> files) {
    return CollectionConverters.asScala(
            files.stream().map(f -> (AdmittableFile) f).collect(Collectors.toList()))
        .toSeq();
  }

  static boolean isCDFEnabled(Metadata metadata) {
    // Fall back to the deprecated delta.enableChangeDataCapture key when
    // delta.enableChangeDataFeed is not set.
    Map<String, String> config = CollectionConverters.asJava(metadata.configuration());
    String value = config.get("delta.enableChangeDataFeed");
    if (value == null) {
      value = config.get("delta.enableChangeDataCapture");
    }
    return value != null && value.equalsIgnoreCase("true");
  }

  /** Checks if the endOffset is at BASE_INDEX for the given version (early exit condition). */
  static boolean isEndOffsetAtBaseIndex(Optional<DeltaSourceOffset> endOffsetOpt, long version) {
    return endOffsetOpt.isPresent()
        && endOffsetOpt.get().reservoirVersion() == version
        && endOffsetOpt.get().index() == DeltaSourceOffset.BASE_INDEX();
  }
}
