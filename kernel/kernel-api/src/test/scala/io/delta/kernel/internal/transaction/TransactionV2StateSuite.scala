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

package io.delta.kernel.internal.transaction

import java.util.Optional

import io.delta.kernel.Operation
import io.delta.kernel.test.{ActionUtils, TransactionV2TestUtils}

import org.scalatest.funsuite.AnyFunSuite

class TransactionV2StateSuite extends AnyFunSuite with ActionUtils with TransactionV2TestUtils {

  test("TransactionV2State constructor throws when no protocol is provided") {
    assertThrows[IllegalArgumentException] {
      createTestTxnState(
        isCreateOrReplace = true,
        Operation.CREATE_TABLE,
        "/path/to/table",
        readTableOpt = Optional.empty(),
        updatedProtocolOpt = Optional.empty(),
        updatedMetadataOpt = Optional.of(basicPartitionedMetadata))
    }
  }

  test("TransactionV2State constructor throws when no metadata is provided") {
    assertThrows[IllegalArgumentException] {
      createTestTxnState(
        isCreateOrReplace = true,
        Operation.CREATE_TABLE,
        "/path/to/table",
        readTableOpt = Optional.empty(),
        updatedProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
        updatedMetadataOpt = Optional.empty())
    }
  }

  // TODO: getEffectiveProtocol, getEffectiveMetadata tests
}
