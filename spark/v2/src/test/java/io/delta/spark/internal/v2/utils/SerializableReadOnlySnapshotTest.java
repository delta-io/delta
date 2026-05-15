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
package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SerializableReadOnlySnapshotTest extends DeltaV2TestBase {

  @TempDir java.io.File tempDir;

  @Test
  public void testSerializationRoundTrip() throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.range(10).write().format("delta").save(tablePath);

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    PathBasedSnapshotManager mgr = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SnapshotImpl snapshot = (SnapshotImpl) mgr.loadLatestSnapshot();

    SerializableReadOnlySnapshot original =
        SerializableReadOnlySnapshot.fromSnapshot(snapshot, hadoopConf);

    byte[] bytes;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
      bytes = baos.toByteArray();
    }

    SerializableReadOnlySnapshot deserialized;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (SerializableReadOnlySnapshot) ois.readObject();
    }

    assertEquals(original.getVersion(), deserialized.getVersion());

    Scan scan = deserialized.toScan();
    assertNotNull(scan);

    Engine engine = DefaultEngine.create(deserialized.getHadoopConf());
    List<FilteredColumnarBatch> batches = new ArrayList<>();
    try (CloseableIterator<FilteredColumnarBatch> iter = scan.getScanFiles(engine)) {
      while (iter.hasNext()) {
        batches.add(iter.next());
      }
    }
    assertFalse(batches.isEmpty(), "Deserialized scan should produce file batches");
  }

  @Test
  public void testToScanReturnsReadOnlyScan() {
    String tablePath = tempDir.getAbsolutePath();
    spark.range(5).write().format("delta").save(tablePath);

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    PathBasedSnapshotManager mgr = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SnapshotImpl snapshot = (SnapshotImpl) mgr.loadLatestSnapshot();

    SerializableReadOnlySnapshot serializable =
        SerializableReadOnlySnapshot.fromSnapshot(snapshot, hadoopConf);

    Scan scan = serializable.toScan();
    assertNotNull(scan);
    assertNotNull(scan.getScanState(defaultEngine));
    assertNotNull(scan.getRemainingFilter());
  }
}
