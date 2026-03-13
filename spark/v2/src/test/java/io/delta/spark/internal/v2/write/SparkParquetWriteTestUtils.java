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

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

final class SparkParquetWriteTestUtils {
  private SparkParquetWriteTestUtils() {}

  static SparkParquetBatchWrite createBatchWrite(
      SparkSession spark, File tempDir, String tableName, String queryId) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        String.format("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", tableName, tablePath));

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.create(tablePath, engine, Optional.empty());
    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();
    StructType writeSchema = new StructType().add("id", DataTypes.IntegerType);

    return new SparkParquetBatchWrite(
        tablePath,
        hadoopConf,
        initialSnapshot,
        writeSchema,
        queryId,
        new HashMap<>(),
        Arrays.asList("id"));
  }
}
