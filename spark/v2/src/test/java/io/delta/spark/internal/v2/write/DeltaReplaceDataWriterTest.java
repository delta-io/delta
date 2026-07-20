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
package io.delta.spark.internal.v2.write;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.types.IntegerType;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.Collections;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class DeltaReplaceDataWriterTest {

  @Test
  public void commitMessageNormalizesAndProtectsCollections() {
    DeltaReplaceDataCommitMessage message = new DeltaReplaceDataCommitMessage(null, null);
    assertNotNull(message.getAddFileActionRows());
    assertNotNull(message.getSourceFilePaths());
    assertTrue(message.getAddFileActionRows().isEmpty());
    assertTrue(message.getSourceFilePaths().isEmpty());
    assertThrows(
        UnsupportedOperationException.class, () -> message.getAddFileActionRows().add(null));
    assertThrows(UnsupportedOperationException.class, () -> message.getSourceFilePaths().add("x"));
  }

  @Test
  public void commitMessagePreservesPayloads() {
    io.delta.kernel.types.StructType schema =
        new io.delta.kernel.types.StructType().add("x", IntegerType.INTEGER);
    Row row = JsonUtils.rowFromJson("{\"x\": 42}", schema);
    SerializableKernelRowWrapper wrapper = new SerializableKernelRowWrapper(row);
    DeltaReplaceDataCommitMessage message =
        new DeltaReplaceDataCommitMessage(
            Collections.singletonList(wrapper), Collections.singleton("source"));
    assertEquals(Collections.singletonList(wrapper), message.getAddFileActionRows());
    assertEquals(Collections.singleton("source"), message.getSourceFilePaths());
  }

  @Test
  public void writerFactoryCreatesReplaceDataWriter() {
    DeltaReplaceDataWriterFactory factory =
        new DeltaReplaceDataWriterFactory(
            null, null, null, null, null, new StructType(), Collections.emptyMap(), "UTC");
    DataWriter<InternalRow> writer = factory.createWriter(1, 2L);
    assertTrue(writer instanceof DeltaReplaceDataWriter);
  }

  @Test
  public void writerRejectsNonStringSourceFilePathMetadata() {
    DeltaReplaceDataWriterFactory factory =
        new DeltaReplaceDataWriterFactory(
            null, null, null, null, null, new StructType(), Collections.emptyMap(), "UTC");
    DataWriter<InternalRow> writer = factory.createWriter(1, 2L);
    InternalRow metadata = new GenericInternalRow(new Object[] {1});

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> writer.write(metadata, InternalRow.empty()));
    assertTrue(e.getMessage().contains("Expected source file path metadata at index 0"));
  }
}
