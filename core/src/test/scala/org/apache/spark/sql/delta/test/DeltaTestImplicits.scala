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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.actions.{Action, Metadata}

/**
 * Additional method definitions for Delta classes that are intended for use only in testing.
 */
object DeltaTestImplicits {
  implicit class OptimisticTxnTestHelper(txn: OptimisticTransaction) {
    /** Ensures that the initial commit of a Delta table always contains a Metadata action */
    def commitManually(actions: Action*): Long = {
      if (txn.readVersion == -1 && !actions.exists(_.isInstanceOf[Metadata])) {
        txn.commit(Metadata() +: actions, ManualUpdate)
      } else {
        txn.commit(actions, ManualUpdate)
      }
    }
  }
}
