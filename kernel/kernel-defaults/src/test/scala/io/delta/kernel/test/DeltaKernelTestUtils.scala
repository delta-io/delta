/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.test

import org.apache.spark.sql.delta.{DeltaOperations, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.Action

/**
 * Custom test utilities to replace missing internal Delta-Spark test APIs
 * when using published delta-spark versions for Option 1.
 *
 * This provides equivalent functionality to DeltaTestImplicits.OptimisticTxnTestHelper
 * which is not available in published delta-spark JARs.
 */
object DeltaKernelTestUtils {

  /**
   * Implicit class that adds test helper methods to OptimisticTransaction
   * to replace the missing DeltaTestImplicits.OptimisticTxnTestHelper
   */
  implicit class OptimisticTxnTestHelper(txn: OptimisticTransaction) {

    /**
     * Custom implementation of commitManually for testing purposes.
     * Uses public APIs to commit transactions manually, equivalent to
     * the internal commitManually method.
     */
    def commitManually(actions: Action*): Long = {
      txn.commit(actions.toSeq, DeltaOperations.ManualUpdate)
    }
  }
}
