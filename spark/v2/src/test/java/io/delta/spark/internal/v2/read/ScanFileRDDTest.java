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
package io.delta.spark.internal.v2.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import io.delta.spark.internal.v2.utils.SerializableReadOnlySnapshot;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ScanFileRDDTest extends DeltaV2TestBase {

  @TempDir java.io.File tempDir;

  @Test
  public void testScanFileRDDProducesAddFileRows() {
    String tablePath = tempDir.getAbsolutePath();
    spark.range(100).repartition(5).write().format("delta").save(tablePath);

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    PathBasedSnapshotManager mgr = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SnapshotImpl snapshot = (SnapshotImpl) mgr.loadLatestSnapshot();

    SerializableReadOnlySnapshot serializable =
        SerializableReadOnlySnapshot.fromSnapshot(snapshot, hadoopConf);

    ScanFileRDD rdd = new ScanFileRDD(spark.sparkContext(), serializable);
    Dataset<Row> df = spark.createDataFrame(rdd, ScanFileRDD.SPARK_SCHEMA);

    List<Row> rows = df.collectAsList();
    assertEquals(5, rows.size(), "Should have 5 parquet files from repartition(5)");

    for (Row row : rows) {
      String path = row.getAs("path");
      assertNotNull(path, "path should not be null");
      assertFalse(path.isEmpty(), "path should not be empty");
    }
  }

  @Test
  public void testScanFileRDDSortable() {
    String tablePath = tempDir.getAbsolutePath();
    spark.range(50).repartition(3).write().format("delta").save(tablePath);

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    PathBasedSnapshotManager mgr = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SnapshotImpl snapshot = (SnapshotImpl) mgr.loadLatestSnapshot();

    SerializableReadOnlySnapshot serializable =
        SerializableReadOnlySnapshot.fromSnapshot(snapshot, hadoopConf);

    ScanFileRDD rdd = new ScanFileRDD(spark.sparkContext(), serializable);
    Dataset<Row> df = spark.createDataFrame(rdd, ScanFileRDD.SPARK_SCHEMA);

    Dataset<Row> sorted = df.orderBy("modificationTime", "path");
    List<Row> rows = sorted.collectAsList();
    assertEquals(3, rows.size());

    for (int i = 1; i < rows.size(); i++) {
      long prevTime = rows.get(i - 1).getAs("modificationTime");
      long currTime = rows.get(i).getAs("modificationTime");
      String prevPath = rows.get(i - 1).getAs("path");
      String currPath = rows.get(i).getAs("path");
      assertTrue(
          prevTime < currTime || (prevTime == currTime && prevPath.compareTo(currPath) <= 0),
          "Rows should be sorted by modificationTime then path");
    }
  }
}
