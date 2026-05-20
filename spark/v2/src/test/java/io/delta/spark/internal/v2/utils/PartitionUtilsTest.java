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

import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;
import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.read.CDCDataFile;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.delta.RowIndexFilterType;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Map$;
import scala.jdk.javaapi.CollectionConverters;

public class PartitionUtilsTest extends DeltaV2TestBase {

  private static final long MB = 1024 * 1024;

  private static final DeletionVectorDescriptor TEST_DV =
      new DeletionVectorDescriptor("u", "ab^-rstxgiarsd", Optional.of(4), 40, 3L);

  private static AddFile createAddFileForCDC(
      String path, long size, long modificationTime, Optional<DeletionVectorDescriptor> dv) {
    return new AddFile(
        AddFile.createAddFileRow(
            new io.delta.kernel.types.StructType(),
            path,
            VectorUtils.stringStringMapValue(Collections.emptyMap()),
            size,
            modificationTime,
            /* dataChange= */ true,
            dv,
            /* tags= */ Optional.empty(),
            /* baseRowId= */ Optional.empty(),
            /* defaultRowCommitVersion= */ Optional.empty(),
            /* stats= */ Optional.empty()));
  }

  private static java.util.Map<String, Object> getFileMetadata(PartitionedFile pf) {
    return CollectionConverters.asJava(pf.otherConstantMetadataColumnValues());
  }

  @Test
  public void testGetPartitionRow_FieldOrdering() {
    // Schema defines order: year, month, day
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("year", DataTypes.IntegerType, true),
              DataTypes.createStructField("month", DataTypes.IntegerType, true),
              DataTypes.createStructField("day", DataTypes.IntegerType, true)
            });

    // map value has different order: day, year, month
    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("day", "25");
    partitionValues.put("year", "2024");
    partitionValues.put("month", "11");

    MapValue mapValue = stringStringMapValue(partitionValues);
    InternalRow row = PartitionUtils.getPartitionRow(mapValue, partitionSchema, ZoneId.of("UTC"));

    // verify order is schema order: year, month, day
    assertEquals(2024, row.getInt(0));
    assertEquals(11, row.getInt(1));
    assertEquals(25, row.getInt(2));
  }

  @Test
  public void testGetPartitionRow_StringValueIsNotDoubleUrlDecoded() {
    // Kernel's MapValue already holds the logical (decoded) string. Spark's
    // castPartValueToDesiredType would unescapePathName again; each case below picks an input
    // whose second-decode result is distinct from the input AND from the other cases' results,
    // so a regression cannot coincidentally agree with the expected value.
    //   "%20"   -> bug result " "   (canonical space-collapse)
    //   "%25"   -> bug result "%"   (self-encoding of `%`)
    //   "a%2Fb" -> bug result "a/b" (embedded percent escape mid-string)
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("p_space", DataTypes.StringType, true),
              DataTypes.createStructField("p_percent", DataTypes.StringType, true),
              DataTypes.createStructField("p_embedded", DataTypes.StringType, true)
            });

    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("p_space", "%20");
    partitionValues.put("p_percent", "%25");
    partitionValues.put("p_embedded", "a%2Fb");

    InternalRow row =
        PartitionUtils.getPartitionRow(
            stringStringMapValue(partitionValues), partitionSchema, ZoneId.of("UTC"));

    assertEquals(UTF8String.fromString("%20"), row.getUTF8String(0));
    assertEquals(UTF8String.fromString("%25"), row.getUTF8String(1));
    assertEquals(UTF8String.fromString("a%2Fb"), row.getUTF8String(2));
  }

  @Test
  public void testGetPartitionRow_SizeMismatchExtraKeys() {
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("year", DataTypes.IntegerType, true),
              DataTypes.createStructField("month", DataTypes.IntegerType, true)
            });

    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("year", "2024");
    partitionValues.put("month", "11");
    partitionValues.put("day", "25");
    partitionValues.put("hour", "10");

    MapValue mapValue = stringStringMapValue(partitionValues);

    assertThrows(
        AssertionError.class,
        () -> PartitionUtils.getPartitionRow(mapValue, partitionSchema, ZoneId.of("UTC")));
  }

  @Test
  public void testGetPartitionRow_SizeMismatchMissingKeys() {
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("year", DataTypes.IntegerType, true),
              DataTypes.createStructField("month", DataTypes.IntegerType, true),
              DataTypes.createStructField("day", DataTypes.IntegerType, true)
            });

    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("year", "2024");
    partitionValues.put("month", "11");

    MapValue mapValue = stringStringMapValue(partitionValues);

    assertThrows(
        AssertionError.class,
        () -> PartitionUtils.getPartitionRow(mapValue, partitionSchema, ZoneId.of("UTC")));
  }

  @Test
  public void testCreateDeltaParquetReaderFactory_Basic() {
    String tablePath = createTestTable("test_delta_reader_factory_" + System.nanoTime(), true);

    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);

    StructType dataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.StringType, true)});
    StructType readDataSchema = dataSchema;
    StructType ddlOrderedReadOutputSchema =
        SchemaUtils.ddlOrderedOutputSchema(
            SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema()),
            readDataSchema,
            partitionSchema);
    Filter[] filters = new Filter[0];
    scala.collection.immutable.Map<String, String> options = Map$.MODULE$.empty();
    Configuration hadoopConf = new Configuration();
    SQLConf sqlConf = SQLConf.get();

    PartitionReaderFactory factory =
        PartitionUtils.createDeltaParquetReaderFactory(
            snapshot,
            dataSchema,
            partitionSchema,
            readDataSchema,
            ddlOrderedReadOutputSchema,
            filters,
            options,
            hadoopConf,
            sqlConf,
            /* isWriteTimeCDCRead */ false);

    assertNotNull(factory, "PartitionReaderFactory should not be null");
  }

  @Test
  public void testCreateDeltaParquetReaderFactory_isCDCRead() {
    String tablePath = createTestTable("test_delta_reader_factory_cdc_" + System.nanoTime(), true);

    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);

    StructType dataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.StringType, true)});
    StructType readDataSchema = dataSchema;
    StructType ddlOrderedReadOutputSchema =
        SchemaUtils.ddlOrderedOutputSchema(
            SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema()),
            readDataSchema,
            partitionSchema);
    Filter[] filters = new Filter[0];
    scala.collection.immutable.Map<String, String> options = Map$.MODULE$.empty();
    Configuration hadoopConf = new Configuration();
    SQLConf sqlConf = SQLConf.get();

    PartitionReaderFactory factory =
        PartitionUtils.createDeltaParquetReaderFactory(
            snapshot,
            dataSchema,
            partitionSchema,
            readDataSchema,
            ddlOrderedReadOutputSchema,
            filters,
            options,
            hadoopConf,
            sqlConf,
            /* isWriteTimeCDCRead */ true);

    assertNotNull(factory, "CDC PartitionReaderFactory should not be null");
  }

  /**
   * Read-time Auto-CDF calls into PartitionUtils with {@code isWriteTimeCDCRead=false}. The factory
   * is then a plain Parquet reader factory: PartitionUtils does not augment {@code readDataSchema}
   * with CDC tail columns and does not wrap the reader with {@code CDCReadFunction}. The tail
   * columns are added by {@code DeltaChangelogBatch.CDCPartitionReaderFactory} as per-partition
   * constants instead.
   */
  @Test
  public void testCreateDeltaParquetReaderFactory_NotWriteTimeCDCRead() {
    String tablePath =
        createTestTable("test_delta_reader_factory_batch_changelog_" + System.nanoTime(), true);

    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);

    StructType dataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.StringType, true)});
    StructType readDataSchema = dataSchema;
    StructType ddlOrderedReadOutputSchema =
        SchemaUtils.ddlOrderedOutputSchema(
            SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema()),
            readDataSchema,
            partitionSchema);
    Filter[] filters = new Filter[0];
    scala.collection.immutable.Map<String, String> options = Map$.MODULE$.empty();
    Configuration hadoopConf = new Configuration();
    SQLConf sqlConf = SQLConf.get();

    PartitionReaderFactory factory =
        PartitionUtils.createDeltaParquetReaderFactory(
            snapshot,
            dataSchema,
            partitionSchema,
            readDataSchema,
            ddlOrderedReadOutputSchema,
            filters,
            options,
            hadoopConf,
            sqlConf,
            /* isWriteTimeCDCRead */ false);

    assertNotNull(factory, "isWriteTimeCDCRead=false PartitionReaderFactory should not be null");
  }

  @Test
  public void testCalculateMaxSplitBytes_Basic() {
    SQLConf sqlConf = SQLConf.get();
    long minPartitionNum = 4;
    sqlConf.setConfString("spark.sql.files.minPartitionNum", String.valueOf(minPartitionNum));

    long totalBytes = 100 * MB;
    int fileCount = 10;

    long result = PartitionUtils.calculateMaxSplitBytes(spark, totalBytes, fileCount, sqlConf);
    long openCostInBytes = sqlConf.filesOpenCostInBytes();
    long maxPartitionBytes = sqlConf.filesMaxPartitionBytes();

    long calculatedTotalBytes = totalBytes + (long) fileCount * openCostInBytes;
    assertEquals(calculatedTotalBytes / minPartitionNum, result);
  }

  @Test
  public void testCalculateMaxSplitBytes_BoundaryConditions() {
    SQLConf sqlConf = SQLConf.get();
    // Set minPartitionNum=1 for predictable calculations
    sqlConf.setConfString("spark.sql.files.minPartitionNum", "1");
    long openCostInBytes = sqlConf.filesOpenCostInBytes();
    long maxPartitionBytes = sqlConf.filesMaxPartitionBytes();

    // Zero files and bytes
    long result1 = PartitionUtils.calculateMaxSplitBytes(spark, 0L, 0, sqlConf);
    assertEquals(openCostInBytes, result1);

    // Single large file (exceeds maxPartitionBytes)
    long result2 = PartitionUtils.calculateMaxSplitBytes(spark, 1000 * MB, 1, sqlConf);
    assertEquals(maxPartitionBytes, result2);

    // Very small totalBytes
    long result3 = PartitionUtils.calculateMaxSplitBytes(spark, 1024L, 1, sqlConf);
    long expected3 = 1024L + openCostInBytes;
    assertEquals(expected3, result3);

    // Many small files
    long result4 = PartitionUtils.calculateMaxSplitBytes(spark, 1 * MB, 1000, sqlConf);
    assertEquals(maxPartitionBytes, result4);
  }

  @Test
  public void testCalculateMaxSplitBytes_UndefinedMinPartitionNum() {
    SQLConf sqlConf = SQLConf.get();
    // Ensure filesMinPartitionNum is undefined
    if (sqlConf.filesMinPartitionNum().isDefined()) {
      sqlConf.unsetConf("spark.sql.files.minPartitionNum");
    }

    long totalBytes = 200 * MB;
    int fileCount = 10;

    long result = PartitionUtils.calculateMaxSplitBytes(spark, totalBytes, fileCount, sqlConf);

    // Verify the result is still valid
    assertTrue(result > 0);
    assertTrue(result >= sqlConf.filesOpenCostInBytes());
    assertTrue(result <= sqlConf.filesMaxPartitionBytes());
    long calculatedTotalBytes = totalBytes + (long) fileCount * sqlConf.filesOpenCostInBytes();
    assertTrue(result <= calculatedTotalBytes);
  }

  @Test
  public void testBuildPartitionedFile() throws Exception {
    String tablePath = createTestTable("test_build_partitioned_file_" + System.nanoTime(), true);

    // Get an AddFile from the table
    Table table = Table.forPath(defaultEngine, tablePath);
    Scan scan = table.getLatestSnapshot(defaultEngine).getScanBuilder().build();
    FilteredColumnarBatch batch = scan.getScanFiles(defaultEngine).next();
    CloseableIterator<Row> rows = batch.getRows();
    AddFile addFile = new AddFile(rows.next().getStruct(0));
    rows.close();

    // Build PartitionedFile
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.StringType, true)});
    String normalizedTablePath = tablePath.endsWith("/") ? tablePath : tablePath + "/";
    PartitionedFile partitionedFile =
        PartitionUtils.buildPartitionedFile(
            addFile, partitionSchema, normalizedTablePath, ZoneId.of("UTC"));

    assertNotNull(partitionedFile);
    assertEquals(addFile.getSize(), partitionedFile.fileSize());
    assertEquals(1, partitionedFile.partitionValues().numFields());
  }

  /**
   * Verifies that Spark {@code _metadata} base fields for a Kernel-built {@link PartitionedFile}
   * align with Delta {@link AddFile} plus table root: path/name via the same {@code Path} + {@link
   * SparkPath#fromUrlString} rule as {@link PartitionUtils#buildPartitionedFile}, and size/block
   * bounds/mtime from the AddFile when using whole-file splits (start=0, length=size).
   */
  @Test
  public void testBuildPartitionedFile_OneShotMetadataBaseFieldsFromAddFile() throws Exception {
    String tablePath = createTestTable("test_one_shot_metadata_" + System.nanoTime(), true);
    Table table = Table.forPath(defaultEngine, tablePath);
    Scan scan = table.getLatestSnapshot(defaultEngine).getScanBuilder().build();
    FilteredColumnarBatch batch = scan.getScanFiles(defaultEngine).next();
    CloseableIterator<Row> rows = batch.getRows();
    AddFile addFile = new AddFile(rows.next().getStruct(0));
    rows.close();

    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.StringType, true)});
    String normalizedTablePath = tablePath.endsWith("/") ? tablePath : tablePath + "/";
    PartitionedFile pf =
        PartitionUtils.buildPartitionedFile(
            addFile, partitionSchema, normalizedTablePath, ZoneId.of("UTC"));

    SparkPath reconstructed =
        SparkPath.fromUrlString(new Path(normalizedTablePath, addFile.getPath()).toString());

    assertEquals(
        FileFormat$.MODULE$.getDisplayPath(reconstructed, true),
        FileFormat$.MODULE$.getDisplayPath(pf.filePath(), true),
        "_metadata.file_path: AddFile.path + tablePath (same as buildPartitionedFile) must match");
    assertEquals(
        FileFormat$.MODULE$.getDisplayName(reconstructed),
        FileFormat$.MODULE$.getDisplayName(pf.filePath()),
        "_metadata.file_name: must match display name derived from the same combined path");

    assertEquals(0L, pf.start());
    assertEquals(addFile.getSize(), pf.length());
    assertEquals(addFile.getSize(), pf.fileSize());
    assertEquals(addFile.getModificationTime(), pf.modificationTime());
  }

  /**
   * Table root containing spaces: {@link PartitionUtils#buildPartitionedFile} still builds a {@link
   * SparkPath} string from {@code Path(table, addFile.path)}, but {@code FileFormat.getDisplayPath}
   * (and thus SQL {@code _metadata.file_path}) parses that string as a URI and can throw {@link
   * URISyntaxException} until the path is URI-safe.
   */
  @Test
  public void testBuildPartitionedFile_SpaceInTableDirectory_GetDisplayPathNotUriSafe()
      throws Exception {
    String unique = "meta_space_" + System.nanoTime();
    java.nio.file.Path parent =
        Paths.get(System.getProperty("java.io.tmpdir"), "delta test dir " + unique);
    Files.createDirectories(parent);
    String tablePath = parent.resolve("tbl").toString();
    spark.range(5).write().format("delta").save(tablePath);

    Table table = Table.forPath(defaultEngine, tablePath);
    Scan scan = table.getLatestSnapshot(defaultEngine).getScanBuilder().build();
    FilteredColumnarBatch batch = scan.getScanFiles(defaultEngine).next();
    CloseableIterator<Row> rows = batch.getRows();
    AddFile addFile = new AddFile(rows.next().getStruct(0));
    rows.close();

    StructType partitionSchema = new StructType(new StructField[0]);
    String normalizedTablePath = tablePath.endsWith("/") ? tablePath : tablePath + "/";
    PartitionedFile pf =
        PartitionUtils.buildPartitionedFile(
            addFile, partitionSchema, normalizedTablePath, ZoneId.of("UTC"));

    SparkPath reconstructed =
        SparkPath.fromUrlString(new Path(normalizedTablePath, addFile.getPath()).toString());
    assertEquals(
        reconstructed.urlEncoded(),
        pf.filePath().urlEncoded(),
        "One-shot path string from AddFile + table root matches PartitionedFile.filePath");

    assertThrows(
        URISyntaxException.class, () -> FileFormat$.MODULE$.getDisplayPath(pf.filePath(), true));
  }

  @Test
  public void testDdlOrderedOutputSchema_PartitionInMiddleInterleavedAtDdlPosition() {
    String tablePath = getTempTablePath("ddl_order_middle_" + System.nanoTime());
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, part LONG, col3 INT) USING delta "
                + "PARTITIONED BY (part)",
            tablePath));
    Snapshot snapshot = Table.forPath(defaultEngine, tablePath).getLatestSnapshot(defaultEngine);

    StructType readDataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("col3", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.LongType, true)});

    StructType result =
        SchemaUtils.ddlOrderedOutputSchema(
            SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema()),
            readDataSchema,
            partitionSchema);
    assertArrayEquals(new String[] {"id", "part", "col3"}, result.fieldNames());
  }

  @Test
  public void testDdlOrderedOutputSchema_PartitionAtEndIsIdentity() {
    String tablePath = getTempTablePath("ddl_order_end_" + System.nanoTime());
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, col3 INT, part LONG) USING delta "
                + "PARTITIONED BY (part)",
            tablePath));
    Snapshot snapshot = Table.forPath(defaultEngine, tablePath).getLatestSnapshot(defaultEngine);

    StructType readDataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("col3", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.LongType, true)});

    StructType result =
        SchemaUtils.ddlOrderedOutputSchema(
            SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema()),
            readDataSchema,
            partitionSchema);
    assertArrayEquals(new String[] {"id", "col3", "part"}, result.fieldNames());
  }

  @Test
  public void testDdlOrderedOutputSchema_NoPartitionsShortCircuits() {
    String tablePath = getTempTablePath("ddl_order_no_part_" + System.nanoTime());
    spark.sql(String.format("CREATE TABLE delta.`%s` (id LONG, col3 INT) USING delta", tablePath));
    Snapshot snapshot = Table.forPath(defaultEngine, tablePath).getLatestSnapshot(defaultEngine);

    StructType readDataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("col3", DataTypes.IntegerType, true)
            });
    StructType emptyPartitions = new StructType(new StructField[0]);

    StructType result =
        SchemaUtils.ddlOrderedOutputSchema(
            SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema()),
            readDataSchema,
            emptyPartitions);
    // Returns the readDataSchema instance unchanged (short-circuit).
    assertSame(readDataSchema, result);
  }

  @Test
  public void testDdlOrderedOutputSchema_MetadataLeftoverAppendedAtEnd() {
    String tablePath = getTempTablePath("ddl_order_meta_" + System.nanoTime());
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, part LONG, col3 INT) USING delta "
                + "PARTITIONED BY (part)",
            tablePath));
    Snapshot snapshot = Table.forPath(defaultEngine, tablePath).getLatestSnapshot(defaultEngine);

    // _metadata is a synthetic Spark column not in the persisted schema; verify it lands at the
    // tail in insertion order rather than being interleaved.
    StructField metadataField =
        DataTypes.createStructField(
            "_metadata",
            new StructType(
                new StructField[] {
                  DataTypes.createStructField("file_path", DataTypes.StringType, true)
                }),
            true);
    StructType readDataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("col3", DataTypes.IntegerType, true),
              metadataField
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.LongType, true)});

    StructType result =
        SchemaUtils.ddlOrderedOutputSchema(
            SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema()),
            readDataSchema,
            partitionSchema);
    assertArrayEquals(new String[] {"id", "part", "col3", "_metadata"}, result.fieldNames());
  }

  @Test
  public void testDdlOrderedOutputSchema_MultiplePartitionsInterleaved() {
    String tablePath = getTempTablePath("ddl_order_multi_" + System.nanoTime());
    // PARTITIONED BY order intentionally differs from DDL order to exercise both axes.
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (a LONG, p1 STRING, b INT, p2 STRING, c DOUBLE) USING delta "
                + "PARTITIONED BY (p2, p1)",
            tablePath));
    Snapshot snapshot = Table.forPath(defaultEngine, tablePath).getLatestSnapshot(defaultEngine);

    StructType readDataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("a", DataTypes.LongType, true),
              DataTypes.createStructField("b", DataTypes.IntegerType, true),
              DataTypes.createStructField("c", DataTypes.DoubleType, true)
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("p2", DataTypes.StringType, true),
              DataTypes.createStructField("p1", DataTypes.StringType, true)
            });

    StructType result =
        SchemaUtils.ddlOrderedOutputSchema(
            SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema()),
            readDataSchema,
            partitionSchema);
    assertArrayEquals(new String[] {"a", "p1", "b", "p2", "c"}, result.fieldNames());
  }

  /** Helper to create a test Delta table. */
  private String createTestTable(String tableName, boolean partitioned) {
    String tablePath = getTempTablePath(tableName);
    if (partitioned) {
      spark
          .range(10)
          .selectExpr("id", "cast(id % 3 as string) as part")
          .write()
          .format("delta")
          .partitionBy("part")
          .save(tablePath);
    } else {
      spark.range(10).write().format("delta").save(tablePath);
    }
    return tablePath;
  }

  private String getTempTablePath(String tableName) {
    return java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"), "delta-test-" + tableName)
        .toString();
  }

  @Test
  public void testBuildCDCPartitionedFile_dvDiff_setsOverrideDvAndIfNotContained() {
    AddFile addFile = createAddFileForCDC("data.parquet", 1024, 100L, Optional.of(TEST_DV));
    CDCDataFile cdcFile =
        CDCDataFile.fromDVDiff(
            addFile,
            CDCReader.CDC_TYPE_DELETE_STRING(),
            /* commitTimestamp= */ 1000L,
            "override==");

    PartitionedFile pf =
        PartitionUtils.buildCDCPartitionedFile(
            cdcFile,
            /* commitVersion= */ 1L,
            new StructType(),
            "file:/tmp/table",
            ZoneId.of("UTC"));

    java.util.Map<String, Object> metadata = getFileMetadata(pf);
    assertEquals(
        "override==", metadata.get(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED()));
    assertEquals(
        RowIndexFilterType.IF_NOT_CONTAINED,
        metadata.get(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE()));
  }

  @Test
  public void testBuildCDCPartitionedFile_regularAddFileWithDv_setsIfContained() {
    AddFile addFile = createAddFileForCDC("data.parquet", 2048, 200L, Optional.of(TEST_DV));
    CDCDataFile cdcFile = CDCDataFile.fromAddFile(addFile, /* commitTimestamp= */ 2000L);

    PartitionedFile pf =
        PartitionUtils.buildCDCPartitionedFile(
            cdcFile,
            /* commitVersion= */ 2L,
            new StructType(),
            "file:/tmp/table",
            ZoneId.of("UTC"));

    java.util.Map<String, Object> metadata = getFileMetadata(pf);
    assertEquals(
        TEST_DV.serializeToBase64(),
        metadata.get(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED()));
    assertEquals(
        RowIndexFilterType.IF_CONTAINED,
        metadata.get(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE()));
  }

  @Test
  public void testBuildCDCPartitionedFile_noDv_noFilterMetadata() {
    AddFile addFile = createAddFileForCDC("data.parquet", 512, 300L, Optional.empty());
    CDCDataFile cdcFile = CDCDataFile.fromAddFile(addFile, /* commitTimestamp= */ 3000L);

    PartitionedFile pf =
        PartitionUtils.buildCDCPartitionedFile(
            cdcFile,
            /* commitVersion= */ 3L,
            new StructType(),
            "file:/tmp/table",
            ZoneId.of("UTC"));

    java.util.Map<String, Object> metadata = getFileMetadata(pf);
    assertFalse(
        metadata.containsKey(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED()),
        "No filter metadata expected when AddFile has no DV");
  }

  @Test
  public void testPlanInputPartitions_EmptyFiles() {
    // An empty file list must return an empty array without throwing.
    List<PartitionedFile> files = new ArrayList<>();
    Configuration hadoopConf = new Configuration();
    SQLConf sqlConf = SQLConf.get();
    InputPartition[] partitions =
        PartitionUtils.planInputPartitions(spark, files, 0L, hadoopConf, sqlConf);
    assertNotNull(partitions, "Result must not be null for empty input");
    assertEquals(0, partitions.length, "Result must be empty for empty file list");
  }

  /** Returns all PartitionedFiles for the files in the given Delta table path. */
  private List<PartitionedFile> getAllPartitionedFiles(String tablePath) throws IOException {
    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);
    Scan scan = snapshot.getScanBuilder().build();
    StructType emptyPartitionSchema = new StructType(new StructField[0]);
    String normalizedPath = tablePath.endsWith("/") ? tablePath : tablePath + "/";

    List<PartitionedFile> files = new ArrayList<>();
    try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(defaultEngine)) {
      while (scanFiles.hasNext()) {
        FilteredColumnarBatch batch = scanFiles.next();
        try (CloseableIterator<io.delta.kernel.data.Row> rows = batch.getRows()) {
          while (rows.hasNext()) {
            AddFile addFile = new AddFile(rows.next().getStruct(0));
            files.add(
                PartitionUtils.buildPartitionedFile(
                    addFile, emptyPartitionSchema, normalizedPath, ZoneId.of("UTC")));
          }
        }
      }
    }
    return files;
  }
}
