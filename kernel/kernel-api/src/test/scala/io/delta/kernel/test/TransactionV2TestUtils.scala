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

package io.delta.kernel.test

import java.util.Optional

import io.delta.kernel.Operation
import io.delta.kernel.internal.actions.{Metadata, Protocol, SetTransaction}
import io.delta.kernel.internal.table.ResolvedTableInternal
import io.delta.kernel.internal.transaction.TransactionV2State
import io.delta.kernel.internal.util.Clock

trait TransactionV2TestUtils {
  protected def createTestTxnState(
      isCreateOrReplace: Boolean,
      operation: Operation,
      dataPath: String,
      readTableOpt: Optional[ResolvedTableInternal],
      updatedProtocolOpt: Optional[Protocol],
      updatedMetadataOpt: Optional[Metadata]): TransactionV2State = {
    new TransactionV2State(
      isCreateOrReplace,
      "engineInfo",
      operation,
      dataPath,
      readTableOpt,
      updatedProtocolOpt,
      updatedMetadataOpt,
      new Clock {
        override def getTimeMillis: Long = System.currentTimeMillis()
      },
      Optional.of(new SetTransaction("appId", 123, Optional.empty() /* lastUpdated */ )))
  }
}
