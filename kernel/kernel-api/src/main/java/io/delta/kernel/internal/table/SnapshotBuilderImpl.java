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

package io.delta.kernel.internal.table;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Snapshot;
import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.files.ParsedDeltaData;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.Tuple2;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of {@link SnapshotBuilder}.
 *
 * <p>Note: The primary responsibility of this class is to take input, validate that input, and then
 * pass the input to the {@link SnapshotFactory}, which is then responsible for actually creating
 * the {@link Snapshot} instance.
 */
public class SnapshotBuilderImpl implements SnapshotBuilder {

  public static class Context {
    public final String unresolvedPath;
    public Optional<Long> versionOpt = Optional.empty();
    public Optional<Tuple2<SnapshotImpl, Long>> timestampQueryContextOpt = Optional.empty();
    public Optional<Committer> committerOpt = Optional.empty();
    public List<ParsedLogData> logDatas = Collections.emptyList();
    public Optional<Tuple2<Protocol, Metadata>> protocolAndMetadataOpt = Optional.empty();

    public Context(String unresolvedPath) {
      this.unresolvedPath = requireNonNull(unresolvedPath, "unresolvedPath is null");
    }
  }

  private final Context ctx;

  public SnapshotBuilderImpl(String unresolvedPath) {
    ctx = new Context(unresolvedPath);
  }

  ////////////////////////////////////
  // Public SnapshotBuilder Methods //
  ////////////////////////////////////

  @Override
  public SnapshotBuilderImpl atVersion(long version) {
    ctx.versionOpt = Optional.of(version);
    return this;
  }

  @Override
  public SnapshotBuilderImpl atTimestamp(long millisSinceEpochUTC, Snapshot latestSnapshot) {
    requireNonNull(latestSnapshot, "latestSnapshot is null");
    checkArgument(latestSnapshot instanceof SnapshotImpl, "latestSnapshot must be a SnapshotImpl");
    ctx.timestampQueryContextOpt =
        Optional.of(new Tuple2<>((SnapshotImpl) latestSnapshot, millisSinceEpochUTC));
    return this;
  }

  @Override
  public SnapshotBuilderImpl withCommitter(Committer committer) {
    ctx.committerOpt = Optional.of(requireNonNull(committer, "committer is null"));
    return this;
  }

  @Override
  public SnapshotBuilderImpl withLogData(List<ParsedLogData> logDatas) {
    ctx.logDatas = requireNonNull(logDatas, "logDatas is null");
    return this;
  }

  @Override
  public SnapshotBuilderImpl withProtocolAndMetadata(Protocol protocol, Metadata metadata) {
    ctx.protocolAndMetadataOpt =
        Optional.of(
            new Tuple2<>(
                requireNonNull(protocol, "protocol is null"),
                requireNonNull(metadata, "metadata is null")));
    return this;
  }

  @Override
  public SnapshotImpl build(Engine engine) {
    validateInputOnBuild(engine);
    return new SnapshotFactory(engine, ctx).create(engine);
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  private void validateInputOnBuild(Engine engine) {
    validateVersionNonNegative();
    validateTimestampNonNegative();
    validateTimestampNotGreaterThanLatestSnapshot(engine);
    validateLogDatasEmptyIfTimeTravelByTimestamp();
    validateVersionAndTimestampMutuallyExclusive();
    validateProtocolAndMetadataOnlyIfVersionProvided();
    validateProtocolRead();
    validateLogDataContainsOnlyRatifiedCommits(); // TODO: delta-io/delta#4765 support other types
    validateLogDataIsSortedContiguous();
  }

  private void validateVersionNonNegative() {
    ctx.versionOpt.ifPresent(x -> checkArgument(x >= 0, "version must be >= 0"));
  }

  private void validateTimestampNonNegative() {
    ctx.timestampQueryContextOpt.ifPresent(x -> checkArgument(x._2 >= 0, "timestamp must be >= 0"));
  }

  /**
   * Recall the semantics of time-travel by timestamp: "If the provided timestamp is after (strictly
   * greater than) the timestamp of the latest version of the table, snapshot resolution will fail."
   */
  private void validateTimestampNotGreaterThanLatestSnapshot(Engine engine) {
    ctx.timestampQueryContextOpt.ifPresent(
        x -> {
          final long latestSnapshotVersion = x._1.getVersion();
          final long latestSnapshotTimestamp = x._1.getTimestamp(engine);
          final long requestedTimestamp = x._2;

          if (requestedTimestamp > latestSnapshotTimestamp) {
            throw DeltaErrors.timestampAfterLatestCommit(
                ctx.unresolvedPath,
                requestedTimestamp,
                latestSnapshotTimestamp,
                latestSnapshotVersion);
          }
        });
  }

  private void validateLogDatasEmptyIfTimeTravelByTimestamp() {
    if (ctx.timestampQueryContextOpt.isPresent() && !ctx.logDatas.isEmpty()) {
      throw new UnsupportedOperationException(
          "Time travel by timestamp with logDatas is not yet implemented");
    }
  }

  private void validateVersionAndTimestampMutuallyExclusive() {
    checkArgument(
        !ctx.timestampQueryContextOpt.isPresent() || !ctx.versionOpt.isPresent(),
        "timestamp and version cannot be provided together");
  }

  private void validateProtocolAndMetadataOnlyIfVersionProvided() {
    checkArgument(
        ctx.versionOpt.isPresent() || !ctx.protocolAndMetadataOpt.isPresent(),
        "protocol and metadata can only be provided if a version is provided");
  }

  private void validateProtocolRead() {
    ctx.protocolAndMetadataOpt.ifPresent(
        x -> TableFeatures.validateKernelCanReadTheTable(x._1, ctx.unresolvedPath));
  }

  private void validateLogDataContainsOnlyRatifiedCommits() {
    for (ParsedLogData logData : ctx.logDatas) {
      checkArgument(
          logData instanceof ParsedDeltaData && logData.isFile(),
          "Only staged ratified commits are supported, but found: " + logData);
    }
  }

  private void validateLogDataIsSortedContiguous() {
    if (ctx.logDatas.size() > 1) {
      for (int i = 1; i < ctx.logDatas.size(); i++) {
        final ParsedLogData prev = ctx.logDatas.get(i - 1);
        final ParsedLogData curr = ctx.logDatas.get(i);
        checkArgument(
            prev.getVersion() + 1 == curr.getVersion(),
            String.format(
                "Log data must be sorted and contiguous, but found: %s and %s", prev, curr));
      }
    }
  }
}
