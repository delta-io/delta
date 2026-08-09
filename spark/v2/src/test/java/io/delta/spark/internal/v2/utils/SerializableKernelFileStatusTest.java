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
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SerializableKernelFileStatusTest {

  @Test
  public void testSerializationRoundTrip() throws Exception {
    FileStatus original = FileStatus.of("/table/_delta_log/00000.json", 1234L, 9999L);
    SerializableKernelFileStatus sfs = SerializableKernelFileStatus.from(original);

    byte[] bytes;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(sfs);
      bytes = baos.toByteArray();
    }

    SerializableKernelFileStatus deserialized;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (SerializableKernelFileStatus) ois.readObject();
    }

    FileStatus result = deserialized.toFileStatus();
    assertEquals(original.getPath(), result.getPath());
    assertEquals(original.getSize(), result.getSize());
    assertEquals(original.getModificationTime(), result.getModificationTime());
  }

  @Test
  public void testListRoundTrip() throws Exception {
    List<FileStatus> originals =
        List.of(
            FileStatus.of("/table/_delta_log/00000.json", 100L, 1000L),
            FileStatus.of("/table/_delta_log/00001.json", 200L, 2000L),
            FileStatus.of("/table/_delta_log/00002.json", 300L, 3000L));

    List<SerializableKernelFileStatus> serializable =
        SerializableKernelFileStatus.fromList(originals);

    byte[] bytes;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(new ArrayList<>(serializable));
      bytes = baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    List<SerializableKernelFileStatus> deserialized;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (List<SerializableKernelFileStatus>) ois.readObject();
    }

    List<FileStatus> result = SerializableKernelFileStatus.toFileStatusList(deserialized);
    assertEquals(originals.size(), result.size());
    for (int i = 0; i < originals.size(); i++) {
      assertEquals(originals.get(i).getPath(), result.get(i).getPath());
      assertEquals(originals.get(i).getSize(), result.get(i).getSize());
      assertEquals(originals.get(i).getModificationTime(), result.get(i).getModificationTime());
    }
  }
}
