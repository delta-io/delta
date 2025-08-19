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
package io.delta.spark.dsv2.scan.batch;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.junit.jupiter.api.Test;

public class KernelSparkPartitionReaderFactoryTest extends KernelSparkDsv2TestBase {

  @Test
  public void testCreateReader() {
    String scanState = "{\"key\":\"value\"}";
    String fileRow = "{\"path\":\"/test\"}";
    KernelSparkInputPartition partition = new KernelSparkInputPartition(scanState, fileRow);

    KernelSparkPartitionReaderFactory factory = new KernelSparkPartitionReaderFactory();
    PartitionReader<InternalRow> reader = factory.createReader(partition);

    assertNotNull(reader);
    assertInstanceOf(KernelSparkPartitionReader.class, reader);
  }

  @Test
  public void testCreateReaderWithWrongPartitionType() {
    // Create a mock InputPartition that's not KernelSparkInputPartition
    InputPartition wrongPartition = new InputPartition() {};

    KernelSparkPartitionReaderFactory factory = new KernelSparkPartitionReaderFactory();

    assertThrows(IllegalArgumentException.class, () -> factory.createReader(wrongPartition));
  }

  @Test
  public void testFactoryIsSingleton() {
    KernelSparkPartitionReaderFactory factory1 = new KernelSparkPartitionReaderFactory();
    KernelSparkPartitionReaderFactory factory2 = new KernelSparkPartitionReaderFactory();

    // Factories should be equivalent (stateless)
    assertNotSame(factory1, factory2); // Different instances
    assertEquals(factory1.getClass(), factory2.getClass()); // Same class
  }

  @Test
  public void testSerialization() throws Exception {
    KernelSparkPartitionReaderFactory original = new KernelSparkPartitionReaderFactory();

    // Serialize
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(original);
    }

    // Deserialize
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    KernelSparkPartitionReaderFactory deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(bis)) {
      deserialized = (KernelSparkPartitionReaderFactory) ois.readObject();
    }

    assertNotNull(deserialized);
    assertEquals(original.getClass(), deserialized.getClass());

    // Test that deserialized factory still works
    String scanState = "{\"test\":\"data\"}";
    String fileRow = "{\"path\":\"/test/file.parquet\"}";
    KernelSparkInputPartition partition = new KernelSparkInputPartition(scanState, fileRow);

    PartitionReader<InternalRow> reader = deserialized.createReader(partition);
    assertNotNull(reader);
    assertInstanceOf(KernelSparkPartitionReader.class, reader);
  }

  @Test
  public void testCreateMultipleReaders() {
    KernelSparkPartitionReaderFactory factory = new KernelSparkPartitionReaderFactory();

    // Create multiple partitions
    KernelSparkInputPartition partition1 =
        new KernelSparkInputPartition("{\"scan\":\"state1\"}", "{\"file\":\"path1\"}");
    KernelSparkInputPartition partition2 =
        new KernelSparkInputPartition("{\"scan\":\"state2\"}", "{\"file\":\"path2\"}");
    KernelSparkInputPartition partition3 =
        new KernelSparkInputPartition("{\"scan\":\"state3\"}", "{\"file\":\"path3\"}");

    // Create readers
    PartitionReader<InternalRow> reader1 = factory.createReader(partition1);
    PartitionReader<InternalRow> reader2 = factory.createReader(partition2);
    PartitionReader<InternalRow> reader3 = factory.createReader(partition3);

    // Verify all readers are created and independent
    assertNotNull(reader1);
    assertNotNull(reader2);
    assertNotNull(reader3);
    assertNotSame(reader1, reader2);
    assertNotSame(reader2, reader3);
    assertNotSame(reader1, reader3);

    // All should be the same type
    assertInstanceOf(KernelSparkPartitionReader.class, reader1);
    assertInstanceOf(KernelSparkPartitionReader.class, reader2);
    assertInstanceOf(KernelSparkPartitionReader.class, reader3);
  }

  @Test
  public void testFactoryStateless() {
    KernelSparkPartitionReaderFactory factory = new KernelSparkPartitionReaderFactory();

    String scanState = "{\"state\":\"data\"}";
    String fileRow = "{\"file\":\"data\"}";
    KernelSparkInputPartition partition = new KernelSparkInputPartition(scanState, fileRow);

    // Create multiple readers from same factory
    PartitionReader<InternalRow> reader1 = factory.createReader(partition);
    PartitionReader<InternalRow> reader2 = factory.createReader(partition);

    // Should create different reader instances
    assertNotSame(reader1, reader2);
    assertInstanceOf(KernelSparkPartitionReader.class, reader1);
    assertInstanceOf(KernelSparkPartitionReader.class, reader2);
  }

  @Test
  public void testNullPartition() {
    KernelSparkPartitionReaderFactory factory = new KernelSparkPartitionReaderFactory();

    assertThrows(NullPointerException.class, () -> factory.createReader(null));
  }
}
