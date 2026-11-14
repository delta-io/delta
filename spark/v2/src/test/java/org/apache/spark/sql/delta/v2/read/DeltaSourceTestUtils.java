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

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.IndexedFile;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import scala.Option;

/**
 * Calls into the V1 Delta log and source APIs for the DSv2 streaming parity tests.
 *
 * <p>Java cannot use Scala default arguments, so every Java call site of these Scala methods must
 * pass the full parameter list. Routing the calls through this class keeps the tests independent of
 * the exact parameter list.
 */
final class DeltaSourceTestUtils {

  private DeltaSourceTestUtils() {}

  /** Updates {@code deltaLog} synchronously and returns the latest snapshot. */
  static Snapshot updateSnapshot(DeltaLog deltaLog) {
    return deltaLog.update(
        /* stalenessAcceptable= */ false,
        /* checkIfUpdatedSinceTs= */ Option.empty(),
        /* catalogTableOpt= */ Option.empty());
  }

  static DeltaSourceOffset sourceOffset(
      String reservoirId, long reservoirVersion, long index, boolean isInitialSnapshot) {
    return new DeltaSourceOffset(reservoirId, reservoirVersion, index, isInitialSnapshot);
  }

  static ClosableIterator<IndexedFile> dsv1FileChanges(
      DeltaSource deltaSource,
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Option<DeltaSourceOffset> endOffset,
      boolean verifyMetadataAction) {
    return deltaSource.getFileChanges(
        fromVersion, fromIndex, isInitialSnapshot, endOffset, verifyMetadataAction);
  }

  static ClosableIterator<IndexedFile> dsv1FileChangesWithRateLimit(
      DeltaSource deltaSource,
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Option<DeltaSource.AdmissionLimits> limits) {
    return deltaSource.getFileChangesWithRateLimit(
        fromVersion, fromIndex, isInitialSnapshot, limits);
  }
}
