/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v2.interop

import org.apache.hadoop.conf.Configuration

/**
 * Composite contract for a process-cached Delta table
 * manager.
 *
 * The composite holds an engine-free kernel snapshot
 * that is safe to cache across requests. Per-request
 * context (Hadoop conf) enters at operation time via
 * [[snapshotManager]], which builds a fresh Kernel
 * Engine scoped to the caller's credentials.
 *
 * Table identity (path, catalog table) is seeded at
 * construction and determines cache-key affinity.
 */
trait DeltaV2TableManager {

  /**
   * Idempotently prevents future acquisitions and
   * releases exclusively owned state when safe.
   *
   * Default no-op: the trait is package-private with a
   * single production implementation that overrides
   * this. Test stubs inherit the no-op safely.
   */
  def retire(): Unit = {}
}
