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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf

class DeltaSourceLargeLogSuite extends DeltaSourceSuite {
  protected override def sparkConf = {
    super.sparkConf.set(DeltaSQLConf.LOG_SIZE_IN_MEMORY_THRESHOLD.key, "0")
  }
}

// Batch sizes 1, 2, and 100 exercise different backfill behaviors in the commit coordinator.
// Batch size 1 triggers a backfill on every commit (commitVersion % 1 == 0), testing the most
// granular backfill path. Batch size 2 triggers backfill every other commit, testing the boundary
// between backfilled and unbackfilled commits. Batch size 100 leaves most commits unbackfilled,
// testing the production-like path where streaming must read from both the commit coordinator
// and the filesystem. This follows the same pattern as other CatalogManaged (CCv2) test suites
// (DeltaLogSuite, DeltaCDCStreamSuite, etc.).

class DeltaSourceLargeLogWithCatalogManagedBatch1Suite
    extends DeltaSourceLargeLogSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)
}

class DeltaSourceLargeLogWithCatalogManagedBatch2Suite
    extends DeltaSourceLargeLogSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(2)
}

class DeltaSourceLargeLogWithCatalogManagedBatch100Suite
    extends DeltaSourceLargeLogSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(100)
}
