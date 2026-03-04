/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.delta.kernel.types.*;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for {@link DeltaSchemaDigest}. */
class DeltaSchemaDigestTest {

  @Test
  void testDigest() {
    StructType schema1 =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    StructType schema2 =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
    StructType schema3 =
        new StructType().add("id1", IntegerType.INTEGER).add("part", StringType.STRING);
    StructType schema4 =
        new StructType().add("id", IntegerType.INTEGER, false).add("part", StringType.STRING);

    assertEquals(new DeltaSchemaDigest(schema1).digest(), new DeltaSchemaDigest(schema2).digest());
    assertNotEquals(
        new DeltaSchemaDigest(schema1).digest(), new DeltaSchemaDigest(schema3).digest());
    assertNotEquals(
        new DeltaSchemaDigest(schema1).digest(), new DeltaSchemaDigest(schema4).digest());
  }
}
