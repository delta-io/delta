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
package org.apache.spark.sql.delta.v2.utils;

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
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.delta.RowIndexFilterType;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;
import org.apache.spark.sql.delta.v2.DeltaV2TestBase;
import org.apache.spark.sql.delta.v2.read.CDCDataFile;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
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

  private static PartitionUtils.PartitionValueCaster utcCaster(StructType partitionSchema) {
    return new PartitionUtils.PartitionValueCaster(partitionSchema, ZoneId.of("UTC"));
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
    InternalRow row = PartitionUtils.getPartitionRow(mapValue, utcCaster(partitionSchema));

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
            stringStringMapValue(partitionValues), utcCaster(partitionSchema));

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
        () -> PartitionUtils.getPartitionRow(mapValue, utcCaster(partitionSchema)));
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
        () -> PartitionUtils.getPartitionRow(mapValue, utcCaster(partitionSchema)));
  }

  /** Partition schema and serialized values whose V1 (non-ANSI cast) reads are pinned below. */
  private static final StructType LENIENT_PARTITION_SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("p_byte", DataTypes.ByteType, true),
            DataTypes.createStructField("p_short", DataTypes.ShortType, true),
            DataTypes.createStructField("p_int", DataTypes.IntegerType, true),
            DataTypes.createStructField("p_long", DataTypes.LongType, true),
            DataTypes.createStructField("p_bool", DataTypes.BooleanType, true),
            DataTypes.createStructField("p_bool_other", DataTypes.BooleanType, true),
            DataTypes.createStructField("p_date", DataTypes.DateType, true),
            DataTypes.createStructField("p_ts", DataTypes.TimestampType, true),
            DataTypes.createStructField("p_dec", DataTypes.createDecimalType(10, 1), true),
            DataTypes.createStructField("p_int_trimmed", DataTypes.IntegerType, true)
          });

  private static Map<String, String> lenientPartitionValues() {
    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("p_byte", "300");
    partitionValues.put("p_short", "70000");
    partitionValues.put("p_int", "abc");
    partitionValues.put("p_long", "99999999999999999999");
    partitionValues.put("p_bool", "yes");
    partitionValues.put("p_bool_other", "maybe");
    partitionValues.put("p_date", "2024-13-45");
    partitionValues.put("p_ts", "not-a-timestamp");
    partitionValues.put("p_dec", "1.55");
    partitionValues.put("p_int_trimmed", " 7 ");
    return partitionValues;
  }

  /** Asserts the values the V1 connector reads for {@link #lenientPartitionValues()}. */
  private static void assertLenientPartitionRow(InternalRow row) {
    // Values that do not parse as the column type (including integral overflow) read as null.
    for (int ordinal : new int[] {0, 1, 2, 3, 5, 6, 7}) {
      assertTrue(row.isNullAt(ordinal), "expected null at ordinal " + ordinal);
    }
    assertTrue(row.getBoolean(4));
    // The value is rounded to the declared scale instead of keeping the string's own scale.
    assertEquals(Decimal.apply(new java.math.BigDecimal("1.6"), 10, 1), row.getDecimal(8, 10, 1));
    assertEquals(7, row.getInt(9));
  }

  @Test
  public void testGetPartitionRow_MalformedValuesMatchV1() {
    // The V1 connector converts partition values with a non-ANSI cast (TahoeFileIndex), so the
    // result must not depend on the session ANSI setting.
    String ansiKey = SQLConf.ANSI_ENABLED().key();
    scala.Option<String> original = spark.conf().getOption(ansiKey);
    try {
      for (String ansiEnabled : new String[] {"true", "false"}) {
        spark.conf().set(ansiKey, ansiEnabled);
        assertLenientPartitionRow(
            PartitionUtils.getPartitionRow(
                stringStringMapValue(lenientPartitionValues()),
                utcCaster(LENIENT_PARTITION_SCHEMA)));
        assertLenientPartitionRow(
            PartitionUtils.getPartitionRow(
                lenientPartitionValues(), utcCaster(LENIENT_PARTITION_SCHEMA)));
        // The V1 AddFile overload takes a Scala map.
        scala.collection.immutable.Map<String, String> scalaPartitionValues = Map$.MODULE$.empty();
        for (Map.Entry<String, String> entry : lenientPartitionValues().entrySet()) {
          scalaPartitionValues = scalaPartitionValues.updated(entry.getKey(), entry.getValue());
        }
        assertLenientPartitionRow(
            PartitionUtils.getPartitionRow(
                scalaPartitionValues, utcCaster(LENIENT_PARTITION_SCHEMA)));
      }
    } finally {
      if (original.isDefined()) {
        spark.conf().set(ansiKey, original.get());
      } else {
        spark.conf().unset(ansiKey);
      }
    }
  }

  @Test
  public void testGetPartitionRow_WellFormedValues() {
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("p_byte", DataTypes.ByteType, true),
              DataTypes.createStructField("p_bool", DataTypes.BooleanType, true),
              DataTypes.createStructField("p_date", DataTypes.DateType, true),
              DataTypes.createStructField("p_ts", DataTypes.TimestampType, true),
              DataTypes.createStructField("p_dec", DataTypes.createDecimalType(5, 2), true),
              DataTypes.createStructField("p_bin", DataTypes.BinaryType, true)
            });
    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("p_byte", "-12");
    partitionValues.put("p_bool", "false");
    partitionValues.put("p_date", "2024-01-02");
    partitionValues.put("p_ts", "2024-01-02 03:04:05.123456");
    partitionValues.put("p_dec", "1.23");
    partitionValues.put("p_bin", "ab");

    InternalRow row =
        PartitionUtils.getPartitionRow(
            stringStringMapValue(partitionValues), utcCaster(partitionSchema));

    assertEquals((byte) -12, row.getByte(0));
    assertFalse(row.getBoolean(1));
    assertEquals((int) LocalDate.of(2024, 1, 2).toEpochDay(), row.getInt(2));
    assertEquals(1704164645123456L, row.getLong(3));
    assertEquals(Decimal.apply(new java.math.BigDecimal("1.23"), 5, 2), row.getDecimal(4, 5, 2));
    assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8), row.getBinary(5));
  }

  @Test
  public void testGetPartitionRow_TimestampUsesCasterTimeZone() {
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("p_ts", DataTypes.TimestampType, true),
              DataTypes.createStructField("p_ts_ntz", DataTypes.TimestampNTZType, true),
              DataTypes.createStructField("p_date", DataTypes.DateType, true)
            });
    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("p_ts", "2024-01-02 03:04:05");
    partitionValues.put("p_ts_ntz", "2024-01-02 03:04:05");
    partitionValues.put("p_date", "2024-01-02");

    InternalRow row =
        PartitionUtils.getPartitionRow(
            partitionValues,
            new PartitionUtils.PartitionValueCaster(
                partitionSchema, ZoneId.of("America/Los_Angeles")));

    // TIMESTAMP is read in the caster's zone (UTC-8 in January): 2024-01-02T11:04:05Z.
    assertEquals(1704193445000000L, row.getLong(0));
    // TIMESTAMP_NTZ and DATE do not depend on the zone.
    assertEquals(1704164645000000L, row.getLong(1));
    assertEquals((int) LocalDate.of(2024, 1, 2).toEpochDay(), row.getInt(2));
  }

  @Test
  public void testGetPartitionRow_ReusedCasterKeepsRowsIndependent() {
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("p_int", DataTypes.IntegerType, true),
              DataTypes.createStructField("p_str", DataTypes.StringType, true),
              DataTypes.createStructField("p_bin", DataTypes.BinaryType, true)
            });
    PartitionUtils.PartitionValueCaster caster =
        new PartitionUtils.PartitionValueCaster(partitionSchema, ZoneId.of("UTC"));
    Map<String, String> first = new HashMap<>();
    first.put("p_int", "1");
    first.put("p_str", "a");
    first.put("p_bin", "x");
    Map<String, String> malformed = new HashMap<>();
    malformed.put("p_int", "abc");
    malformed.put("p_str", null);
    malformed.put("p_bin", "y");
    Map<String, String> last = new HashMap<>();
    last.put("p_int", "3");
    last.put("p_str", "c");
    last.put("p_bin", "z");

    InternalRow firstRow = PartitionUtils.getPartitionRow(first, caster);
    InternalRow malformedRow = PartitionUtils.getPartitionRow(malformed, caster);
    InternalRow lastRow = PartitionUtils.getPartitionRow(last, caster);

    // Earlier rows are not changed by later casts with the same caster.
    assertEquals(1, firstRow.getInt(0));
    assertEquals(UTF8String.fromString("a"), firstRow.getUTF8String(1));
    assertArrayEquals("x".getBytes(StandardCharsets.UTF_8), firstRow.getBinary(2));
    assertTrue(malformedRow.isNullAt(0));
    assertTrue(malformedRow.isNullAt(1));
    assertArrayEquals("y".getBytes(StandardCharsets.UTF_8), malformedRow.getBinary(2));
    // A malformed value leaves no state behind for the next file.
    assertEquals(3, lastRow.getInt(0));
    assertEquals(UTF8String.fromString("c"), lastRow.getUTF8String(1));
    assertArrayEquals("z".getBytes(StandardCharsets.UTF_8), lastRow.getBinary(2));
    // A reused caster gives the same row as a fresh one.
    assertEquals(PartitionUtils.getPartitionRow(last, utcCaster(partitionSchema)), lastRow);
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
   * Read-time CDF calls into PartitionUtils with {@code isWriteTimeCDCRead=false}. The factory is
   * then a plain Parquet reader factory: PartitionUtils does not augment {@code readDataSchema}
   * with CDC tail columns and does not wrap the reader with {@code CDCReadFunction}. The tail
   * columns are added by the read-time CDF partition reader as per-partition constants instead.
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
            addFile, utcCaster(partitionSchema), normalizedTablePath);

    assertNotNull(partitionedFile);
    assertEquals(addFile.getSize(), partitionedFile.fileSize());
    assertEquals(1, partitionedFile.partitionValues().numFields());
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
            cdcFile, /* commitVersion= */ 1L, utcCaster(new StructType()), "file:/tmp/table");

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
            cdcFile, /* commitVersion= */ 2L, utcCaster(new StructType()), "file:/tmp/table");

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
            cdcFile, /* commitVersion= */ 3L, utcCaster(new StructType()), "file:/tmp/table");

    java.util.Map<String, Object> metadata = getFileMetadata(pf);
    assertFalse(
        metadata.containsKey(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED()),
        "No filter metadata expected when AddFile has no DV");
  }
}
