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
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.util.Collections;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeltaRowLevelOperationBuilderTest extends DeltaV2TestBase {

  @Test
  public void regularTableDoesNotExposeRowLevelOperations(@TempDir File tempDir) {
    DeltaV2Table table = createBaseTable(tempDir);

    assertFalse(table instanceof SupportsRowLevelOperations);
  }

  @Test
  public void tableExposesRowLevelOperationBuilder(@TempDir File tempDir) {
    DeltaV2Table table = createRowLevelTable(tempDir);

    assertTrue(table instanceof SupportsRowLevelOperations);
    RowLevelOperationBuilder builder =
        ((SupportsRowLevelOperations) table)
            .newRowLevelOperationBuilder(testInfo(RowLevelOperation.Command.DELETE));
    assertTrue(builder instanceof DeltaRowLevelOperationBuilder);
  }

  @Test
  public void buildReturnsCopyOnWriteOperation(@TempDir File tempDir) {
    RowLevelOperation operation = createBuilder(tempDir, RowLevelOperation.Command.DELETE).build();

    assertEquals(RowLevelOperation.Command.DELETE, operation.command());
    assertEquals("DeltaCopyOnWrite(DELETE)", operation.description());
  }

  @Test
  public void operationCommandMatchesInfo(@TempDir File tempDir) {
    for (RowLevelOperation.Command command : RowLevelOperation.Command.values()) {
      RowLevelOperation operation = createBuilder(tempDir, command).build();
      assertEquals(command, operation.command());
    }
  }

  @Test
  public void operationScanBuilderDelegatesToTableAndCapturesScan(@TempDir File tempDir) {
    RowLevelOperation operation = createBuilder(tempDir, RowLevelOperation.Command.DELETE).build();

    ScanBuilder scanBuilder =
        operation.newScanBuilder(new CaseInsensitiveStringMap(Collections.emptyMap()));
    Scan scan = scanBuilder.build();

    assertNotNull(scan);
    assertEquals("DeltaV2Scan", scan.getClass().getSimpleName());
  }

  @Test
  public void requiredMetadataAttributesIncludeMetadataColumn(@TempDir File tempDir) {
    RowLevelOperation operation = createBuilder(tempDir, RowLevelOperation.Command.DELETE).build();

    NamedReference[] metadataAttributes = operation.requiredMetadataAttributes();
    assertEquals(1, metadataAttributes.length);
    assertArrayEquals(
        new String[] {FileFormat$.MODULE$.METADATA_NAME()}, metadataAttributes[0].fieldNames());
  }

  @Test
  public void writeBuilderIsDeferredToCopyOnWriteWriterPr(@TempDir File tempDir) {
    RowLevelOperation operation = createBuilder(tempDir, RowLevelOperation.Command.DELETE).build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> operation.newWriteBuilder(new TestLogicalWriteInfo(tableSchema())));
  }

  @Test
  public void constructorRejectsNull(@TempDir File tempDir) {
    DeltaV2Table table = createRowLevelTable(tempDir);
    RowLevelOperationInfo info = testInfo(RowLevelOperation.Command.DELETE);
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    Snapshot snapshot =
        new PathBasedSnapshotManager(tempDir.getAbsolutePath(), engine).loadLatestSnapshot();

    assertThrows(
        NullPointerException.class,
        () -> new DeltaRowLevelOperationBuilder(null, engine, hadoopConf, snapshot, info));
    assertThrows(
        NullPointerException.class,
        () -> new DeltaRowLevelOperationBuilder(table, null, hadoopConf, snapshot, info));
    assertThrows(
        NullPointerException.class,
        () -> new DeltaRowLevelOperationBuilder(table, engine, null, snapshot, info));
    assertThrows(
        NullPointerException.class,
        () -> new DeltaRowLevelOperationBuilder(table, engine, hadoopConf, null, info));
    assertThrows(
        NullPointerException.class,
        () -> new DeltaRowLevelOperationBuilder(table, engine, hadoopConf, snapshot, null));
  }

  private DeltaV2Table createBaseTable(File tableDir) {
    String path = tableDir.getAbsolutePath();
    String tableName = "delta_row_level_" + System.nanoTime();
    createEmptyTestTable(path, tableName);
    return new DeltaV2Table(Identifier.of(new String[] {"default"}, tableName), path);
  }

  private DeltaV2Table createRowLevelTable(File tableDir) {
    String path = tableDir.getAbsolutePath();
    String tableName = "delta_row_level_" + System.nanoTime();
    createEmptyTestTable(path, tableName);
    return new RowLevelDeltaV2Table(Identifier.of(new String[] {"default"}, tableName), path);
  }

  private DeltaRowLevelOperationBuilder createBuilder(
      File tableDir, RowLevelOperation.Command command) {
    DeltaV2Table table = createRowLevelTable(new File(tableDir, "table_" + System.nanoTime()));
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    Snapshot snapshot =
        new PathBasedSnapshotManager(table.getTablePath().toString(), engine).loadLatestSnapshot();
    return new DeltaRowLevelOperationBuilder(
        table, engine, hadoopConf, snapshot, testInfo(command));
  }

  private static class RowLevelDeltaV2Table extends DeltaV2Table
      implements SupportsRowLevelOperations {
    RowLevelDeltaV2Table(Identifier identifier, String tablePath) {
      super(identifier, tablePath);
    }
  }

  private static StructType tableSchema() {
    return new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  }

  private static RowLevelOperationInfo testInfo(RowLevelOperation.Command command) {
    return new RowLevelOperationInfo() {
      @Override
      public CaseInsensitiveStringMap options() {
        return new CaseInsensitiveStringMap(Collections.emptyMap());
      }

      @Override
      public RowLevelOperation.Command command() {
        return command;
      }
    };
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

    @Override
    public Optional<StructType> metadataSchema() {
      return Optional.of(
          new StructType()
              .add(
                  FileFormat$.MODULE$.METADATA_NAME(),
                  new StructType().add("file_path", DataTypes.StringType)));
    }
  }
}
