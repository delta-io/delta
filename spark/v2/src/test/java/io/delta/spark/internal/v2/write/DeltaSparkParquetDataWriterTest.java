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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

public class DeltaSparkParquetDataWriterTest extends DeltaV2TestBase {

  @Test
  public void commitWithNoRowsReturnsEmptyActionRows(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    createEmptyTestTable(path, "t");

    DeltaSparkParquetDataWriter writer = createWriter(path);

    DeltaWriterCommitMessage msg = commitAndCast(writer);
    assertTrue(msg.getActionRows().isEmpty());
  }

  @Test
  public void commitWithRowsCreatesParquetAndReturnsActionRows(@TempDir File tempDir)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    createEmptyTestTable(path, "t_with_rows");

    DeltaSparkParquetDataWriter writer = createWriter(path);

    InternalRow row1 = new GenericInternalRow(new Object[] {1, UTF8String.fromString("Alice")});
    InternalRow row2 = new GenericInternalRow(new Object[] {2, UTF8String.fromString("Bob")});
    writer.write(row1);
    writer.write(row2);

    DeltaWriterCommitMessage msg = commitAndCast(writer);
    List<SerializableKernelRowWrapper> actionRows = msg.getActionRows();
    assertEquals(1, actionRows.size());
  }

  private DeltaSparkParquetDataWriter createWriter(String path) throws Exception {
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(path, hadoopConf);
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    Engine engine = DefaultEngine.create(hadoopConf);
    Transaction txn = snapshot.buildUpdateTableTransaction("test", Operation.WRITE).build(engine);
    Row txnState = txn.getTransactionState(engine);
    SerializableKernelRowWrapper serializedTxn = new SerializableKernelRowWrapper(txnState);

    Map<String, io.delta.kernel.expressions.Literal> partitionValues = Collections.emptyMap();
    DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);
    String targetDir = writeContext.getTargetDirectory();

    StructType tableSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
    Job job = Job.getInstance(hadoopConf);
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    DeltaParquetFileFormatV2 format =
        new DeltaParquetFileFormatV2(
            snapshotImpl.getProtocol(),
            snapshotImpl.getMetadata(),
            /* nullableRowTrackingConstantFields */ false,
            /* nullableRowTrackingGeneratedFields */ false,
            /* optimizationsEnabled */ true,
            Option.apply(path),
            /* isCDCRead */ false,
            /* useMetadataRowIndex */ Option.empty());
    OutputWriterFactory outputWriterFactory =
        format.prepareWrite(spark, job, ScalaUtils.toScalaMap(Collections.emptyMap()), tableSchema);
    SerializableConfiguration serConf = new SerializableConfiguration(job.getConfiguration());

    return new DeltaSparkParquetDataWriter(
        targetDir, serConf, serializedTxn, tableSchema, outputWriterFactory, 0, 0);
  }

  private static DeltaWriterCommitMessage commitAndCast(DeltaSparkParquetDataWriter writer)
      throws Exception {
    WriterCommitMessage msg = writer.commit();
    assertNotNull(msg);
    assertTrue(msg instanceof DeltaWriterCommitMessage);
    return (DeltaWriterCommitMessage) msg;
  }
}
