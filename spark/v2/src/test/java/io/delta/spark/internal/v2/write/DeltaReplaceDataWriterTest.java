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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.IntegerType;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.read.DeltaParquetFileFormatV2;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
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

public class DeltaReplaceDataWriterTest extends DeltaV2TestBase {

  @Test
  public void twoArgWriteTracksSourceFilePaths(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    createEmptyTestTable(path, "replace_data_tracks_paths");

    DeltaReplaceDataWriter writer = createWriter(path);

    GenericInternalRow metadataRow = new GenericInternalRow(1);
    metadataRow.update(0, UTF8String.fromString("file:/tmp/source-file.parquet"));
    InternalRow dataRow = new GenericInternalRow(new Object[] {1, UTF8String.fromString("Alice")});
    writer.write(metadataRow, dataRow);

    DeltaReplaceDataCommitMessage message = commitAndCast(writer);
    assertEquals(
        Collections.singleton("file:/tmp/source-file.parquet"), message.getSourceFilePaths());
    assertEquals(1, message.getAddFileActionRows().size());
  }

  @Test
  public void singleArgWriteDoesNotTrackPaths(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    createEmptyTestTable(path, "replace_data_no_paths");

    DeltaReplaceDataWriter writer = createWriter(path);
    writer.write(new GenericInternalRow(new Object[] {1, UTF8String.fromString("Alice")}));

    DeltaReplaceDataCommitMessage message = commitAndCast(writer);
    assertTrue(message.getSourceFilePaths().isEmpty());
    assertEquals(1, message.getAddFileActionRows().size());
  }

  @Test
  public void commitProducesAddFileActions(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    createEmptyTestTable(path, "replace_data_add_file");

    DeltaReplaceDataWriter writer = createWriter(path);
    writer.write(new GenericInternalRow(new Object[] {1, UTF8String.fromString("Alice")}));
    writer.write(new GenericInternalRow(new Object[] {2, UTF8String.fromString("Bob")}));

    DeltaReplaceDataCommitMessage message = commitAndCast(writer);
    assertEquals(1, message.getAddFileActionRows().size());

    Row singleActionRow = message.getAddFileActionRows().get(0).getRow();
    int addOrdinal = singleActionRow.getSchema().indexOf("add");
    assertFalse(singleActionRow.isNullAt(addOrdinal));
    Row addFileRow = singleActionRow.getStruct(addOrdinal);
    // AddFile.path is table-relative per the Delta protocol (e.g. "part-00000-...parquet").
    String filePath = addFileRow.getString(0);
    long fileSize = addFileRow.getLong(2);
    int dataChangeOrdinal = addFileRow.getSchema().indexOf("dataChange");
    assertNotNull(filePath);
    assertTrue(
        filePath.endsWith(".parquet"), "Expected AddFile path to end with .parquet: " + filePath);
    assertFalse(
        filePath.startsWith("/") || filePath.contains("://"),
        "AddFile path must be table-relative, not absolute: " + filePath);
    assertTrue(fileSize > 0L);
    assertTrue(
        addFileRow.getBoolean(dataChangeOrdinal),
        "ReplaceData AddFile must be marked dataChange=true");
    // Resolve the relative path against the test-table directory and verify the file exists.
    assertTrue(
        Files.exists(Paths.get(path, filePath)),
        "Expected the Parquet file referenced by the AddFile to exist on disk under " + path);
  }

  @Test
  public void emptyWriteProducesEmptyCommit(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    createEmptyTestTable(path, "replace_data_empty_commit");

    DeltaReplaceDataWriter writer = createWriter(path);

    DeltaReplaceDataCommitMessage message = commitAndCast(writer);
    assertTrue(message.getAddFileActionRows().isEmpty());
    assertTrue(message.getSourceFilePaths().isEmpty());
  }

  @Test
  public void commitMessageTreatsNullCollectionsAsEmpty() {
    DeltaReplaceDataCommitMessage message = new DeltaReplaceDataCommitMessage(null, null);
    assertNotNull(message.getAddFileActionRows());
    assertNotNull(message.getSourceFilePaths());
    assertTrue(message.getAddFileActionRows().isEmpty());
    assertTrue(message.getSourceFilePaths().isEmpty());
  }

  @Test
  public void commitMessageReturnsUnmodifiableViews() {
    DeltaReplaceDataCommitMessage message =
        new DeltaReplaceDataCommitMessage(Collections.emptyList(), Collections.emptySet());
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
            Collections.singletonList(wrapper), Collections.singleton("p"));
    assertEquals(1, message.getAddFileActionRows().size());
    assertEquals(wrapper, message.getAddFileActionRows().get(0));
    assertEquals(Collections.singleton("p"), message.getSourceFilePaths());
  }

  private DeltaReplaceDataWriter createWriter(String path) throws Exception {
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(path, hadoopConf);
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    Engine engine = DefaultEngine.create(hadoopConf);
    Transaction txn = snapshot.buildUpdateTableTransaction("test", Operation.WRITE).build(engine);
    Row txnState = txn.getTransactionState(engine);
    SerializableKernelRowWrapper serializedTxn = new SerializableKernelRowWrapper(txnState);

    DataWriteContext writeContext =
        Transaction.getWriteContext(engine, txnState, Collections.emptyMap());
    String targetDir = writeContext.getTargetDirectory();

    StructType dataSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
    Job job = Job.getInstance(hadoopConf);
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    DeltaParquetFileFormatV2 format =
        new DeltaParquetFileFormatV2(
            snapshotImpl.getProtocol(),
            snapshotImpl.getMetadata(),
            false,
            false,
            true,
            Option.apply(path),
            false,
            Option.empty());
    OutputWriterFactory outputWriterFactory =
        format.prepareWrite(spark, job, ScalaUtils.toScalaMap(Collections.emptyMap()), dataSchema);
    SerializableConfiguration serConf = new SerializableConfiguration(job.getConfiguration());

    return new DeltaReplaceDataWriter(
        targetDir,
        serConf,
        serializedTxn,
        dataSchema,
        outputWriterFactory,
        new StructType(),
        Collections.emptyMap(),
        spark.sessionState().conf().sessionLocalTimeZone(),
        0,
        0);
  }

  private static DeltaReplaceDataCommitMessage commitAndCast(DeltaReplaceDataWriter writer)
      throws Exception {
    WriterCommitMessage message = writer.commit();
    assertNotNull(message);
    assertTrue(message instanceof DeltaReplaceDataCommitMessage);
    return (DeltaReplaceDataCommitMessage) message;
  }
}
