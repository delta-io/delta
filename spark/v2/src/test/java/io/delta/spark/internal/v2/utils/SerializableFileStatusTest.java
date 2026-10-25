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
package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.kernel.utils.FileStatus;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.Test;

public class SerializableFileStatusTest {

  @Test
  public void testSerializationRoundTrip() throws Exception {
    FileStatus original = FileStatus.of("/table/_delta_log/00000.json", 1234L, 9999L);
    SerializableFileStatus sfs = SerializableFileStatus.from(original);

    byte[] bytes;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(sfs);
      bytes = baos.toByteArray();
    }

    SerializableFileStatus deserialized;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (SerializableFileStatus) ois.readObject();
    }

    FileStatus result = deserialized.toFileStatus();
    assertEquals(original.getPath(), result.getPath());
    assertEquals(original.getSize(), result.getSize());
    assertEquals(original.getModificationTime(), result.getModificationTime());
  }
}
