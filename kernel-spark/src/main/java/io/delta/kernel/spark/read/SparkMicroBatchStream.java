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

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder.CommitBoundary;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.ColumnVector;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.DeltaUnsupportedTableFeatureException;
import org.apache.spark.sql.delta.StartingVersion;
import org.apache.spark.sql.delta.StartingVersionLatest$;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.DeltaTimeTravelSpec;
import org.apache.spark.sql.catalyst.expressions.Literal;
import scala.Option;
import scala.Some;
import scala.util.control.NonFatal;

public class SparkMicroBatchStream implements MicroBatchStream {

  private static final Logger logger = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private final String tablePath;
  private final Map<String, String> options;
  private final Engine engine;
  private final DeltaOptions deltaOptions;
  private final SparkSession spark;

  /** Snapshot at the time this source was initialized. */
  private final Snapshot snapshotAtSourceInit;
  private final String tableId;

  public SparkMicroBatchStream(
      String tablePath, 
      Map<String, String> options,
      SparkSession spark) {
    this.tablePath = tablePath;
    this.options = options;
    this.engine = DefaultEngine.create();
    this.spark = spark;
    this.deltaOptions = new DeltaOptions(options, spark.sessionState().conf());

    this.snapshotAtSourceInit = TableManager.loadSnapshot(tablePath).build(engine);
    this.tableId = snapshotAtSourceInit.getMetadata().getId();
  }

  ////////////
  // offset //
  ////////////

  @Override
  public Offset initialOffset() {
    Long startingVersion = getStartingVersion();

    if (startingVersion == null) {
      // No starting version specified, start from the latest snapshot
      return getStartingOffsetFromSpecificVersion(snapshotAtSourceInit.getVersion(), /*isInitialSnapshot=*/ true);
    } else if (startingVersion < 0) {
      // Negative version means no offset should be returned
      return null;
    } else {
      // Use the specified starting version
      return getStartingOffsetFromSpecificVersion(startingVersion, /*isInitialSnapshot=*/ false);
    }
  }

  /**
   * Extracts the user-provided options to time travel to a specific version or timestamp. Supports
   * options: startingVersion and startingTimestamp.
   *
   * @return Starting version, or null if should start from latest
   */
  private Long getStartingVersion() {
    if (deltaOptions.startingVersion().isDefined()) {
      Object startingVersionObj = deltaOptions.startingVersion().get();
      if (startingVersionObj instanceof StartingVersionLatest$) {
        // TODO(parity): use kernel.update()
        Snapshot latestSnapshot = TableManager.loadSnapshot(tablePath).build(engine);
        return latestSnapshot.getVersion() + 1;
      } else if (startingVersionObj instanceof StartingVersion) {
        StartingVersion sv = (StartingVersion) startingVersionObj;
        long version = sv.version();
        validateProtocolAt(spark, version)
        // TODO(parity): need Kernel API equivalent for deltaLog.history.checkVersionExists
        return version;
      } else {
        throw DeltaErrors.illegalDeltaOptionException(
            DeltaOptions.STARTING_VERSION_OPTION(),
            startingVersionObj.toString(),
            "must be a number or 'latest'");
      }
    } else if (deltaOptions.startingTimestamp().isDefined()) {
      DeltaTimeTravelSpec tt = new DeltaTimeTravelSpec(
          Option.apply(Literal.apply(deltaOptions.startingTimestamp().get())), //
          Option.empty(), // version
          new Some<>("SparkMicroBatchStream") // creationSource
      );
      String timestampStr = tt.getTimestamp(spark.sessionState().conf());
      try {
        Timestamp timestamp = Timestamp.valueOf(timestampStr);
        return getStartingVersionFromTimestamp(timestamp);
      } catch (IllegalArgumentException e) {
        throw DeltaErrors.invalidTimestampFormat(timestampStr, "yyyy-MM-dd HH:mm:ss[.S]", Option.apply(e));
      }
    }

    return null;
  }

  /**
   * <p>This method analyzes file changes from a specific version and creates an appropriate offset
   * based on the last file change found. 
   * 
   * @return DeltaSourceOffset or null if no file changes exist
   */
  private DeltaSourceOffset getStartingOffsetFromSpecificVersion(
      long fromVersion, boolean isInitialSnapshot) {
    // TODO(parity): Implement this method
    return null;
  }

  /**
   * - If a commit version exactly matches the provided timestamp, we return it.
   * - Otherwise, we return the earliest commit version
   *   with a timestamp greater than the provided one.
   * - If the provided timestamp is larger than the timestamp
   *   of any committed version, we throw an error.
   * <p>When the timestamp exceeds the latest commit timestamp, this method throws an exception
   * matching the DSv1 behavior (when DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP=false, which is the default).
   */
  private Long getStartingVersionFromTimestamp(Timestamp timestamp) {
    // TODO(parity): support canExceedLatest when CDF is supported
    long millisSinceEpoch = timestamp.getTime();

    // TODO(parity): consider using Kernel's getActiveCommitAtTime if supported.
    try {
      Snapshot snapshotAtTimestamp = TableManager.loadSnapshot(tablePath)
          .atTimestamp(millisSinceEpoch, snapshotAtSourceInit)
          .build(engine);
      return snapshotAtTimestamp.getVersion();
    } catch (Exception kernelException) {
      // Handle the case where timestamp exceeds latest commit timestamp
      // Re-throw using DSv1-compatible error to maintain same user experience
      // Use snapshotAtSourceInit (latest snapshot) timestamp for the error message
      throw DeltaErrors.timestampGreaterThanLatestCommit(
          timestamp, 
          new Timestamp(snapshotAtSourceInit.getTimestamp()),
          timestamp.toString());
    }
  }

  /**
   * Validate the protocol at a given version. If the snapshot reconstruction fails for any other
   * reason than table feature exception, we suppress it. This allows to fallback to previous
   * behavior where the starting version/timestamp was not mandatory to point to reconstructable
   * snapshot.
   *
   * Returns true when the validation was performed and succeeded.
   */
  public boolean validateProtocolAt(
      SparkSession spark,
      long version) {
    boolean alwaysValidateProtocol = spark.sessionState().conf().getConf(
        DeltaSQLConf.FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL());
    if (!alwaysValidateProtocol) return false;

    try {
      // We attempt to construct a snapshot at the startingVersion in order to validate the
      // protocol. If snapshot reconstruction fails, fall back to the old behavior where the
      // only requirement was for the commit to exist.
      TableManager.loadSnapshot(this.tablePath, version).build(this.engine);
      return true;
    } catch (DeltaUnsupportedTableFeatureException e) {
      // TODO(parity): loadSnapshot() needs to throw DeltaUnsupportedTableFeatureException
      // TODO(parity): recordDeltaEvent
      throw e;
    } catch (Exception e) {
      logger.error("Protocol validation failed", e);
      // TODO(parity): recordDeltaEvent
      // TODO(parity): do not throw errors after we support checkVersionExists() in Kernel
      throw e;
    }
    return false;
  }

  @Override
  public Offset latestOffset() {
    throw new UnsupportedOperationException("latestOffset is not supported");
  }

  @Override
  public Offset deserializeOffset(String json) {
    throw new UnsupportedOperationException("deserializeOffset is not supported");
  }

  ////////////
  /// data ///
  ////////////

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    throw new UnsupportedOperationException("planInputPartitions is not supported");
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("createReaderFactory is not supported");
  }

  ///////////////
  // lifecycle //
  ///////////////

  @Override
  public void commit(Offset end) {
    throw new UnsupportedOperationException("commit is not supported");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("stop is not supported");
  }
}
