/*
 * Copyright (2026) The Delta Lake Project Authors.
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


package org.apache.spark.sql

import org.apache.spark.sql.delta.Snapshot

package object delta {
  /**
   * Temporary non-load-bearing alias for the current concrete V1 DeltaLog-backed [[Snapshot]].
   *
   * The long-term goal is for `Snapshot` to become the common semantic snapshot abstraction used by
   * Delta connectors. That common abstraction should cover table identity and metadata, constructed
   * table state. Those APIs can eventually be backed by Kernel.
   *
   * Some existing call sites depend on physical DeltaLog implementation details instead:
   * commit/checkpoint files exposed through `logSegment`. These APIs do not belong on the common
   * snapshot abstraction because they will not line up with Kernel-backed snapshots and may not be
   * extensible to future table formats such as Iceberg V4.
   *
   * Use this alias to pre-factor those V1-coupled call sites explicitly before renaming the current
   * concrete [[Snapshot]] implementation to `SnapshotV1`.
   */
  type SnapshotV1 = Snapshot
}
