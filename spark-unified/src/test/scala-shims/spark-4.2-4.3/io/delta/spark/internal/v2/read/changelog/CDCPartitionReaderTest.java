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
package io.delta.spark.internal.v2.read.changelog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import scala.collection.immutable.Map$;

/**
 * Unit test for {@link DeltaV2ChangelogBatch.CDCPartitionReader}, the executor-side reader that
 * iterates every {@link PartitionedFile} bundled into one bin-packed {@link FilePartition}, opens a
 * delegate reader per file, and joins that file's CDC tail ({@code _change_type},
 * {@code _commit_version}, {@code _commit_timestamp}) onto each of its rows.
 *
 * <p>This test drives the reader with a fake {@link PartitionReaderFactory} that returns canned
 * rows and counts {@code close()} calls, so it exercises the multi-file path without a Spark
 * session or an end-to-end read-time-CDF query.
 */
public class CDCPartitionReaderTest {

  /** Output schema fed to the reader: one data column plus the three CDC tail columns. */
  private static final StructType OUTPUT_SCHEMA =
      new StructType()
          .add("id", DataTypes.LongType, false)
          .add("_change_type", DataTypes.StringType, false)
          .add("_commit_version", DataTypes.LongType, false)
          .add("_commit_timestamp", DataTypes.TimestampType, false);

  /** A single data row carrying one long column. */
  private static InternalRow dataRow(long id) {
    GenericInternalRow row = new GenericInternalRow(1);
    row.setLong(0, id);
    return row;
  }

  /**
   * Build a {@link PartitionedFile} whose constant-metadata map holds only the CDC tail, mirroring
   * what {@code DeltaV2ChangelogBatch.buildPartition} packs for the reader to recover per file.
   */
  private static PartitionedFile cdcFile(
      int index, String changeType, long commitVersion, long commitTimestampMicros) {
    scala.collection.immutable.Map<String, Object> meta =
        (scala.collection.immutable.Map<String, Object>)
            (scala.collection.immutable.Map<?, ?>) Map$.MODULE$.empty();
    meta = meta.$plus(new Tuple2<>(CDCSchemaContext.CDC_TYPE_COLUMN, changeType));
    meta = meta.$plus(new Tuple2<>(CDCSchemaContext.CDC_COMMIT_VERSION, commitVersion));
    meta = meta.$plus(new Tuple2<>(CDCSchemaContext.CDC_COMMIT_TIMESTAMP, commitTimestampMicros));
    return new PartitionedFile(
        new GenericInternalRow(0),
        SparkPath.fromUrlString("file:/tmp/changelog-test/f" + index + ".parquet"),
        /* start */ 0L,
        /* length */ 1L,
        /* locations */ new String[0],
        /* modificationTime */ 0L,
        /* fileSize */ 1L,
        meta);
  }

  /** Fake per-file reader over a fixed row list that records how often it is closed. */
  private static final class FakeReader implements PartitionReader<InternalRow> {
    private final Iterator<InternalRow> rows;
    private InternalRow current;
    int closeCount = 0;

    FakeReader(List<InternalRow> rows) {
      this.rows = rows.iterator();
    }

    @Override
    public boolean next() {
      if (rows.hasNext()) {
        current = rows.next();
        return true;
      }
      return false;
    }

    @Override
    public InternalRow get() {
      return current;
    }

    @Override
    public void close() {
      closeCount++;
    }
  }

  /** Fake factory that hands out one {@link FakeReader} per delegate open, in file order. */
  private static final class FakeFactory implements PartitionReaderFactory {
    private final List<List<InternalRow>> rowsPerFile;
    private final List<FakeReader> created = new ArrayList<>();
    private int nextFile = 0;

    FakeFactory(List<List<InternalRow>> rowsPerFile) {
      this.rowsPerFile = rowsPerFile;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      FakeReader reader = new FakeReader(rowsPerFile.get(nextFile++));
      created.add(reader);
      return reader;
    }
  }

  /** One emitted row, values copied out eagerly (the projection reuses its UnsafeRow buffer). */
  private static final class EmittedRow {
    private final long id;
    private final String changeType;
    private final long commitVersion;
    private final long commitTimestamp;

    EmittedRow(long id, String changeType, long commitVersion, long commitTimestamp) {
      this.id = id;
      this.changeType = changeType;
      this.commitVersion = commitVersion;
      this.commitTimestamp = commitTimestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof EmittedRow)) {
        return false;
      }
      EmittedRow that = (EmittedRow) o;
      return id == that.id
          && commitVersion == that.commitVersion
          && commitTimestamp == that.commitTimestamp
          && Objects.equals(changeType, that.changeType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, changeType, commitVersion, commitTimestamp);
    }

    @Override
    public String toString() {
      return "EmittedRow{id=" + id + ", changeType=" + changeType + ", commitVersion="
          + commitVersion + ", commitTimestamp=" + commitTimestamp + "}";
    }
  }

  private static List<EmittedRow> drain(DeltaV2ChangelogBatch.CDCPartitionReader reader)
      throws IOException {
    List<EmittedRow> out = new ArrayList<>();
    while (reader.next()) {
      InternalRow row = reader.get();
      out.add(
          new EmittedRow(
              row.getLong(0),
              row.getUTF8String(1).toString(),
              row.getLong(2),
              row.getLong(3)));
    }
    return out;
  }

  @Test
  public void multipleFilesEmitAllRowsWithPerFileTail() throws IOException {
    PartitionedFile insertFile = cdcFile(0, "insert", 7L, 700L);
    PartitionedFile deleteFile = cdcFile(1, "delete", 8L, 800L);
    FilePartition partition =
        new FilePartition(0, new PartitionedFile[] {insertFile, deleteFile});

    List<List<InternalRow>> rowsPerFile =
        List.of(
            List.of(dataRow(10L), dataRow(11L)),
            List.of(dataRow(20L)));
    FakeFactory factory = new FakeFactory(rowsPerFile);

    DeltaV2ChangelogBatch.CDCPartitionReader reader =
        new DeltaV2ChangelogBatch.CDCPartitionReader(factory, partition, OUTPUT_SCHEMA);
    List<EmittedRow> emitted = drain(reader);
    reader.close();

    // All rows from both files are emitted, each stamped with its own file's tail.
    assertEquals(
        List.of(
            new EmittedRow(10L, "insert", 7L, 700L),
            new EmittedRow(11L, "insert", 7L, 700L),
            new EmittedRow(20L, "delete", 8L, 800L)),
        emitted);

    // Every delegate reader is closed exactly once. A regression that closes the exhausted
    // reader in next() without clearing it double-closes the last file here.
    assertEquals(2, factory.created.size());
    for (FakeReader r : factory.created) {
      assertEquals(1, r.closeCount, "each delegate reader must be closed exactly once");
    }
  }

  @Test
  public void emptyMiddleFileIsSkipped() throws IOException {
    FilePartition partition =
        new FilePartition(
            0,
            new PartitionedFile[] {
              cdcFile(0, "insert", 1L, 100L),
              cdcFile(1, "insert", 2L, 200L),
              cdcFile(2, "delete", 3L, 300L)
            });

    List<List<InternalRow>> rowsPerFile =
        List.of(List.of(dataRow(1L)), List.of(), List.of(dataRow(3L)));
    FakeFactory factory = new FakeFactory(rowsPerFile);

    DeltaV2ChangelogBatch.CDCPartitionReader reader =
        new DeltaV2ChangelogBatch.CDCPartitionReader(factory, partition, OUTPUT_SCHEMA);
    List<EmittedRow> emitted = drain(reader);
    reader.close();

    // The empty middle file contributes no rows but is still opened and closed once.
    assertEquals(
        List.of(new EmittedRow(1L, "insert", 1L, 100L), new EmittedRow(3L, "delete", 3L, 300L)),
        emitted);
    assertEquals(3, factory.created.size());
    for (FakeReader r : factory.created) {
      assertEquals(1, r.closeCount);
    }
  }

  @Test
  public void emptyPartitionEmitsNothing() throws IOException {
    FilePartition partition = new FilePartition(0, new PartitionedFile[0]);
    FakeFactory factory = new FakeFactory(List.of());

    DeltaV2ChangelogBatch.CDCPartitionReader reader =
        new DeltaV2ChangelogBatch.CDCPartitionReader(factory, partition, OUTPUT_SCHEMA);
    assertFalse(reader.next(), "no files means no rows");
    reader.close();
    assertEquals(0, factory.created.size());
  }
}
