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
package io.delta.spark.internal.v2.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class DeltaWriterCommitMessageTest {

  @Test
  public void emptyActionRows() {
    DeltaWriterCommitMessage msg = new DeltaWriterCommitMessage(Collections.emptyList());
    assertNotNull(msg.getActionRows());
    assertTrue(msg.getActionRows().isEmpty());
  }

  @Test
  public void nullActionRowsTreatedAsEmpty() {
    DeltaWriterCommitMessage msg = new DeltaWriterCommitMessage(null);
    assertNotNull(msg.getActionRows());
    assertTrue(msg.getActionRows().isEmpty());
  }

  @Test
  public void preservesNonEmptyActionRows() {
    StructType schema = new StructType().add("x", IntegerType.INTEGER);
    String schemaJson = DataTypeJsonSerDe.serializeDataType(schema);
    io.delta.kernel.data.Row row =
        JsonUtils.rowFromJson("{\"x\": 42}", DataTypeJsonSerDe.deserializeStructType(schemaJson));
    SerializableKernelRowWrapper wrapper = new SerializableKernelRowWrapper(row);

    DeltaWriterCommitMessage msg = new DeltaWriterCommitMessage(Arrays.asList(wrapper));
    assertEquals(1, msg.getActionRows().size());
    assertEquals(wrapper, msg.getActionRows().get(0));
  }

  @Test
  public void getActionRowsReturnsUnmodifiableList() {
    DeltaWriterCommitMessage msg = new DeltaWriterCommitMessage(Collections.emptyList());
    assertThrows(UnsupportedOperationException.class, () -> msg.getActionRows().add(null));
  }
}
