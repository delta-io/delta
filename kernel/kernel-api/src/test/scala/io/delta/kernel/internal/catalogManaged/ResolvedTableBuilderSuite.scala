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

package io.delta.kernel.internal.catalogManaged

import java.util.Collections

import io.delta.kernel.TableManager
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.internal.table.ResolvedTableInternal
import io.delta.kernel.test.{ActionUtils, MockFileSystemClientUtils}
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class ResolvedTableBuilderSuite extends AnyFunSuite
    with MockFileSystemClientUtils with ActionUtils {

  test("if P & M are provided then LogSegment is not loaded") {
    val testSchema = new StructType().add("c1", IntegerType.INTEGER)
    val engine = createMockFSListFromEngine(Nil)

    val resolvedTable = TableManager
      .loadTable(dataPath.toString)
      .atVersion(13)
      .withProtocolAndMetadata(new Protocol(1, 2), testMetadata(testSchema))
      .withLogData(Collections.emptyList())
      .build(engine)
      .asInstanceOf[ResolvedTableInternal]

    assert(!resolvedTable.getLazyLogSegment.isPresent)
  }

  // TODO: Mock JSON reading and then actually read the P & M
}
