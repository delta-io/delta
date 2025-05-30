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

package io.delta.kernel.defaults.test

import io.delta.kernel.{ResolvedTable, Snapshot}
import io.delta.kernel.internal.SnapshotImpl

object TestAdapterImplicits {
  implicit class SnapshotTestOps(private val snapshot: Snapshot) extends AnyVal {
    def toTestAdapter: SnapshotTestAdapter = snapshot match {
      case impl: SnapshotImpl => impl.toTestAdapter
      case _ => throw new IllegalArgumentException("Snapshot must be an instance of SnapshotImpl")
    }
  }

  implicit class ResolvedTableTestOps(private val resolvedTable: ResolvedTable) extends AnyVal {
    def toTestAdapter: ResolvedTableTestAdapter = new ResolvedTableTestAdapter(resolvedTable)
  }
}
