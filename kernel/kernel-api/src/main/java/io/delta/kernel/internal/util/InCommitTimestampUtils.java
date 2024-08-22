/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.TableConfig.isICTEnabled;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InCommitTimestampUtils {

  /**
   * Returns the updated {@link Metadata} with inCommitTimestamp enablement related info (version
   * and timestamp) correctly set. This is done only 1. If this transaction enables
   * inCommitTimestamp. 2. If the commit version is not 0. This is because we only need to persist
   * the enablement info if there are non-ICT commits in the Delta log. Note: This function must
   * only be called after transaction conflicts have been resolved.
   */
  public static Optional<Metadata> getUpdatedMetadataWithICTEnablementInfo(
      Engine engine,
      long inCommitTimestamp,
      SnapshotImpl readSnapshot,
      Metadata metadata,
      long commitVersion) {
    if (didCurrentTransactionEnableICT(engine, metadata, readSnapshot) && commitVersion != 0) {
      Map<String, String> enablementTrackingProperties = new HashMap<>();
      enablementTrackingProperties.put(
          TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey(),
          Long.toString(commitVersion));
      enablementTrackingProperties.put(
          TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey(),
          Long.toString(inCommitTimestamp));

      Metadata newMetadata = metadata.withNewConfiguration(enablementTrackingProperties);
      return Optional.of(newMetadata);
    } else {
      return Optional.empty();
    }
  }

  /** Returns true if the current transaction implicitly/explicitly enables ICT. */
  private static boolean didCurrentTransactionEnableICT(
      Engine engine, Metadata currentTransactionMetadata, SnapshotImpl readSnapshot) {
    // If ICT is currently enabled, and the read snapshot did not have ICT enabled,
    // then the current transaction must have enabled it.
    // In case of a conflict, any winning transaction that enabled it after
    // our read snapshot would have caused a metadata conflict abort
    // (see {@link ConflictChecker.handleMetadata}), so we know that
    // all winning transactions' ICT enablement status must match the read snapshot.
    //
    // WARNING: To ensure that this function returns true if ICT is enabled during the first
    // commit, we explicitly handle the case where the readSnapshot.version is -1.
    boolean isICTCurrentlyEnabled = isICTEnabled(engine, currentTransactionMetadata);
    boolean wasICTEnabledInReadSnapshot =
        readSnapshot.getVersion(engine) != -1 && isICTEnabled(engine, readSnapshot.getMetadata());
    return isICTCurrentlyEnabled && !wasICTEnabledInReadSnapshot;
  }
}
