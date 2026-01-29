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

package io.delta.kernel;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.files.ParsedLogData;
import java.util.List;
import java.util.Optional;

/**
 * A builder for creating {@link CommitRange} instances that define a contiguous range of commits in
 * a Delta Lake table.
 *
 * <p>The start boundary is required and provided via {@link TableManager#loadCommitRange(String,
 * CommitBoundary)}. If no end specification is provided, the range defaults to the latest available
 * version.
 *
 * @since 3.4.0
 */
@Experimental
public interface CommitRangeBuilder {

  /**
   * Configures the builder to end the commit range at a specific version or timestamp.
   *
   * <p>If not specified, the commit range will default to ending at the latest available version.
   *
   * @param endBoundary the boundary specification for the end of the commit range, must not be null
   * @return this builder instance configured with the specified end boundary
   */
  CommitRangeBuilder withEndBoundary(CommitBoundary endBoundary);

  /**
   * Provides parsed log data to optimize the commit range construction.
   *
   * <p><strong>Note:</strong> If no end boundary is provided via {@link
   * #withEndBoundary(CommitBoundary)}, or a timestamp-based end boundary is provided, the provided
   * log data must include all available ratified commits. If a version-based end boundary is
   * provided, the log data must include commits up to at least the end version (i.e., the tail of
   * the log data must have a version greater than or equal to the end version).
   *
   * @param logData the list of pre-parsed log data, must not be null
   * @return this builder instance configured with the specified log data
   */
  // TODO: should we change this to take in a ParsedDeltaData instead?
  CommitRangeBuilder withLogData(List<ParsedLogData> logData);

  /**
   * Specifies the maximum table version known by the catalog.
   *
   * <p>This method is used by catalog implementations for catalog-managed Delta tables to indicate
   * the latest ratified version of the table. This ensures that any commit range operations respect
   * the catalog's view of the table state.
   *
   * <p>Important: This method is required for catalog-managed tables and must not be used for
   * file-system managed tables.
   *
   * <p>When specified, the following additional constraints are enforced:
   *
   * <ul>
   *   <li>When the provided startBoundary is version-based, the start version must be less than or
   *       equal to the max catalog version.
   *   <li>If {@link #withEndBoundary(CommitBoundary)} is used with a version, the requested version
   *       must be less than or equal to the max catalog version.
   *   <li>If the provided startBoundary is timestamp-based, or {@link
   *       #withEndBoundary(CommitBoundary)} is used with a timestamp, the provided latest snapshot
   *       must have a version equal to the max catalog version.
   *   <li>If {@link #withLogData(List)} is provided and no end boundary is specified (resolving to
   *       latest), the log data must end with the max catalog version.
   * </ul>
   *
   * @param version the maximum table version known by the catalog (must be {@code >= 0})
   * @return a new builder instance with the specified max catalog version
   * @throws IllegalArgumentException if version is negative
   */
  CommitRangeBuilder withMaxCatalogVersion(long version);

  /**
   * Builds and returns a {@link CommitRange} instance with the configured specifications.
   *
   * <p>This method validates the builder configuration and constructs the commit range by resolving
   * version numbers from timestamps if necessary and determining the actual commit files that fall
   * within the specified range.
   *
   * @param engine the {@link Engine} to use for file system operations and log parsing
   * @return a new {@link CommitRange} instance configured according to this builder's
   *     specifications
   * @throws IllegalArgumentException if the builder configuration is invalid (e.g., start version
   *     {@code >} end version)
   */
  CommitRange build(Engine engine);

  /**
   * Defines a boundary (start or end) of a commit range in a Delta Lake table.
   *
   * <p>A {@code CommitBoundary} can be based on either a specific version number or a timestamp.
   * When using timestamps, the boundary requires the latest snapshot to help resolve the timestamp
   * to the appropriate version.
   *
   * <p>Use the static factory methods {@link #atVersion(long)} and {@link #atTimestamp(long,
   * Snapshot)} to create instances.
   */
  final class CommitBoundary {

    /**
     * Creates a commit boundary based on a specific version number.
     *
     * @param version the commit version number, must be non-negative
     * @return a new {@code CommitBoundary} representing the specified version
     * @throws IllegalArgumentException if {@code version} is negative
     */
    public static CommitBoundary atVersion(long version) {
      checkArgument(version >= 0, "Version must be >= 0, but got: %d", version);
      return new CommitBoundary(true, version, Optional.empty());
    }

    /**
     * Creates a commit boundary based on a timestamp.
     *
     * <p>The timestamp represents a point in time, and the boundary will resolve to the appropriate
     * commit version.
     *
     * @param timestamp the timestamp in milliseconds since epoch
     * @param latestSnapshot the latest snapshot of the table, used for timestamp resolution
     * @return a new {@code CommitBoundary} representing the specified timestamp
     */
    public static CommitBoundary atTimestamp(long timestamp, Snapshot latestSnapshot) {
      checkArgument(
          latestSnapshot instanceof SnapshotImpl,
          "latestSnapshot must be instance of SnapshotImpl");
      return new CommitBoundary(false, timestamp, Optional.of(latestSnapshot));
    }

    private final boolean isVersion;
    private final long value;
    private final Optional<Snapshot> latestSnapshot;

    private CommitBoundary(boolean isVersion, long value, Optional<Snapshot> latestSnapshot) {
      checkArgument(isVersion || latestSnapshot.isPresent());
      this.isVersion = isVersion;
      this.value = value;
      this.latestSnapshot = latestSnapshot;
    }

    /** @return {@code true} if this is a version-based boundary, {@code false} otherwise */
    public boolean isVersion() {
      return isVersion;
    }

    /** @return {@code true} if this is a timestamp-based boundary, {@code false} otherwise */
    public boolean isTimestamp() {
      return !isVersion;
    }

    /**
     * Returns the version number for version-based boundaries. Callers should check {@link
     * CommitBoundary#isVersion()} before access.
     *
     * @return the version number
     * @throws IllegalStateException if this boundary is timestamp-based
     */
    public long getVersion() {
      if (!isVersion) {
        throw new IllegalStateException("This boundary is not version-based");
      }
      return value;
    }

    /**
     * Returns the timestamp for timestamp-based boundaries. Callers should check {@link
     * CommitBoundary#isTimestamp()} before access.
     *
     * @return the timestamp in milliseconds since epoch
     * @throws IllegalStateException if this boundary is version-based
     */
    public long getTimestamp() {
      if (isVersion) {
        throw new IllegalStateException("This boundary is not timestamp-based");
      }
      return value;
    }

    /**
     * Returns the latest snapshot used for timestamp resolution in timestamp-based boundaries.
     * Callers should check {@link CommitBoundary#isTimestamp()} before access.
     *
     * @return the latest snapshot
     * @throws IllegalStateException if this boundary is version-based
     */
    public Snapshot getLatestSnapshot() {
      if (isVersion) {
        throw new IllegalStateException("This boundary is not timestamp-based");
      }
      return latestSnapshot.get();
    }

    @Override
    public String toString() {
      if (isVersion) {
        return String.format("CommitBoundary{version=%d}", value);
      } else {
        return String.format(
            "CommitBoundary{timestamp=%d, latestSnapshot=%s}", value, latestSnapshot.get());
      }
    }
  }
}
