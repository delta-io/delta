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

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.catalog.DeltaV2Table;
import io.delta.spark.internal.v2.read.SparkScan;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

public class DeltaReplaceDataBatchWriteTest extends DeltaV2TestBase {

  @Test
  public void commitWithNoTaskMessagesRemovesAllSelectedFiles(@TempDir File tempDir)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    String tableName = "replace_data_remove_all_" + System.nanoTime();
    createTestTableWithData(path, tableName);

    SparkScan scan = createScan(tableName, path);
    DeltaReplaceDataBatchWrite batchWrite =
        buildBatchWrite(scan, path, nonPartitionedTableSchema());

    long addFilesBefore =
        org.apache.spark.sql.delta.DeltaLog.forTable(spark, path)
            .update(false, Option.empty(), Option.empty())
            .allFiles()
            .count();
    assertTrue(addFilesBefore > 0L, "Test setup expected at least one AddFile");

    batchWrite.commit(new WriterCommitMessage[0]);

    assertEquals(0L, spark.read().format("delta").load(path).count());
    // Delta-log level regression: the commit must actually have removed the scan-selected AddFiles,
    // not just produced an empty query result by some other means.
    long addFilesAfter =
        org.apache.spark.sql.delta.DeltaLog.forTable(spark, path)
            .update(false, Option.empty(), Option.empty())
            .allFiles()
            .count();
    assertEquals(
        0L,
        addFilesAfter,
        "Expected all AddFiles to be removed from the Delta log after ReplaceData commit");
  }

  @Test
  public void commitUsesRuntimeFilteredScanFilesForRemoveSet(@TempDir File tempDir)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    String tableName = "replace_data_runtime_filter_" + System.nanoTime();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, city STRING) USING delta "
                + "PARTITIONED BY (city) LOCATION '%s'",
            tableName, path));
    spark.sql(
        String.format("INSERT INTO %s VALUES (1, 'Alice', 'hz'), (2, 'Bob', 'la')", tableName));

    SparkScan scan = createScan(tableName, path);
    scan.filter(
        new Predicate[] {
          new Predicate(
              "=",
              new Expression[] {
                FieldReference.apply("city"), LiteralValue.apply("hz", DataTypes.StringType)
              })
        });

    DeltaReplaceDataBatchWrite batchWrite = buildBatchWrite(scan, path, partitionedTableSchema());
    batchWrite.commit(new WriterCommitMessage[0]);

    List<Row> remainingRows =
        spark.read().format("delta").load(path).select("id", "city").collectAsList();
    assertEquals(1, remainingRows.size());
    assertEquals(2, remainingRows.get(0).getInt(0));
    assertEquals("la", remainingRows.get(0).getString(1));
  }

  private SparkScan createScan(String tableName, String path) {
    DeltaV2Table table = new DeltaV2Table(Identifier.of(new String[] {"default"}, tableName), path);
    ScanBuilder scanBuilder =
        table.newScanBuilder(new CaseInsensitiveStringMap(Collections.emptyMap()));
    Scan scan = scanBuilder.build();
    assertTrue(scan instanceof SparkScan);
    return (SparkScan) scan;
  }

  private DeltaReplaceDataBatchWrite buildBatchWrite(
      SparkScan scan, String path, StructType schema) {
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    Snapshot initialSnapshot = new PathBasedSnapshotManager(path, engine).loadLatestSnapshot();
    WriteBuilder writeBuilder =
        new DeltaReplaceDataWriteBuilder(
            engine,
            path,
            hadoopConf,
            initialSnapshot,
            scan::getSelectedFiles,
            new TestLogicalWriteInfo(schema));
    Write write = writeBuilder.build();
    BatchWrite batchWrite = write.toBatch();
    assertTrue(batchWrite instanceof DeltaReplaceDataBatchWrite);
    return (DeltaReplaceDataBatchWrite) batchWrite;
  }

  private static StructType nonPartitionedTableSchema() {
    return new StructType()
        .add("id", DataTypes.IntegerType)
        .add("name", DataTypes.StringType)
        .add("value", DataTypes.DoubleType);
  }

  private static StructType partitionedTableSchema() {
    return new StructType()
        .add("id", DataTypes.IntegerType)
        .add("name", DataTypes.StringType)
        .add("city", DataTypes.StringType);
  }

  private static class TestLogicalWriteInfo implements LogicalWriteInfo {
    private final StructType schema;

    TestLogicalWriteInfo(StructType schema) {
      this.schema = schema;
    }

    @Override
    public String queryId() {
      return "test-query-id";
    }

    @Override
    public StructType schema() {
      return schema;
    }

    @Override
    public CaseInsensitiveStringMap options() {
      return new CaseInsensitiveStringMap(Collections.emptyMap());
    }
  }
}
