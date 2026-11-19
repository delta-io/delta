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
package org.apache.spark.sql.delta.v2.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.stats.DataSize;
import org.apache.spark.sql.delta.stats.DeltaScan;
import org.apache.spark.sql.delta.v2.catalog.DeltaV2Table;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Option;

public class DeltaV2BatchTest extends DeltaV2PartitionedTableTestBase {

  @BeforeAll
  public static void setupPartitionedTable(@TempDir File tempDir) {
    setUpSharedPartitionedTable("deltatbl_partitioned_batch", tempDir);
  }

  @ParameterizedTest
  @ValueSource(strings = {"table with space", "literal%20table", "space and %25literal"})
  public void testBatchDiagnosticsUseCanonicalTablePath(String directory, @TempDir File tempDir)
      throws Exception {
    String path = new File(tempDir, directory).getAbsolutePath();
    spark.range(3).write().format("delta").save(path);
    DeltaV2Table pathTable =
        new DeltaV2Table(
            Identifier.of(new String[] {"default"}, "batch_diagnostic_path"),
            path,
            Collections.emptyMap());
    DeltaV2Scan scan =
        asDeltaV2Scan(((DeltaV2ScanBuilder) pathTable.newScanBuilder(options)).build());
    String canonicalPath = scan.preparedScan().scannedSnapshot().dataPath().toUri().toString();
    Batch batch = scan.toBatch();

    List<LogEvent> events =
        captureBatchLogs(() -> assertTrue(batch.planInputPartitions().length > 0));

    assertEquals(1, events.size());
    LogEvent summary = events.get(0);
    assertEquals(Level.DEBUG, summary.getLevel());
    String message = summary.getMessage().getFormattedMessage();
    assertTrue(message.startsWith("Planned Delta V2 input partitions for "));
    assertTrue(message.contains(canonicalPath), "diagnostics should retain canonical URI encoding");
    assertEquals(3L, spark.sql(String.format("SELECT * FROM dsv2.delta.`%s`", path)).count());
  }

  @ParameterizedTest
  @ValueSource(strings = {"aggregate", "files", "empty"})
  public void testSelectedFileBytesPreservesAggregateOrUsesActualFiles(String accounting) {
    DeltaV2Scan scan = asDeltaV2Scan(((DeltaV2ScanBuilder) table.newScanBuilder(options)).build());
    DeltaScan original = scan.preparedScan();
    scala.collection.immutable.Seq<AddFile> files =
        accounting.equals("empty")
            ? scala.jdk.javaapi.CollectionConverters.asScala(Collections.<AddFile>emptyList())
                .toList()
            : original.files();
    long fileBytes =
        scala.jdk.javaapi.CollectionConverters.asJava(files).stream()
            .mapToLong(AddFile::size)
            .sum();
    Option<Object> aggregate =
        accounting.equals("aggregate") ? Option.apply(fileBytes + 17L) : Option.empty();
    DataSize scanned =
        new DataSize(
            aggregate,
            original.scanned().rows(),
            original.scanned().files(),
            original.scanned().logicalRows());
    DeltaScan selected =
        original.copy(
            original.version(),
            files,
            original.total(),
            original.partition(),
            scanned,
            original.scannedSnapshot(),
            original.partitionFilters(),
            original.dataFilters(),
            original.partitionLikeDataFilters(),
            original.rewrittenPartitionLikeDataFilters(),
            original.unusedFilters(),
            original.scanDurationMs(),
            original.scanDurationNs(),
            original.dataSkippingType());

    assertEquals(
        accounting.equals("aggregate") ? fileBytes + 17L : fileBytes,
        DeltaV2Scan$.MODULE$.selectedFileBytes(selected));
  }

  private List<LogEvent> captureBatchLogs(ThrowingRunnable action) throws Exception {
    Logger logger = (Logger) LogManager.getLogger(DeltaV2Batch.class);
    List<LogEvent> events = new CopyOnWriteArrayList<>();
    AbstractAppender appender =
        new AbstractAppender("batch-diagnostics", null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    Level previousLevel = logger.getLevel();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.DEBUG);
    try {
      action.run();
      return events;
    } finally {
      logger.setLevel(previousLevel);
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  // Cases where two batches built from the same table must be equal. city/date are partition
  // columns; name/cnt are data columns. Both kinds flow into DeltaV2Batch.filterSet for
  // equals/hashCode, but only data-column filters are passed to the Parquet reader.
  static Stream<Arguments> equalBatchCasesProvider() {
    Filter cityEq = new EqualTo("city", "hz");
    Filter dateEq = new EqualTo("date", "20180520");
    Filter nameEq = new EqualTo("name", "x");
    Filter cntGt = new GreaterThan("cnt", 10);
    return Stream.of(
        Arguments.of("no filters", new Filter[0], new Filter[0]),
        Arguments.of("same single filter", new Filter[] {cityEq}, new Filter[] {cityEq}),
        Arguments.of(
            "partition filters in different order",
            new Filter[] {cityEq, dateEq},
            new Filter[] {dateEq, cityEq}),
        Arguments.of(
            "data filters in different order",
            new Filter[] {nameEq, cntGt},
            new Filter[] {cntGt, nameEq}));
  }

  @ParameterizedTest(name = "{0}: batches should be equal")
  @MethodSource("equalBatchCasesProvider")
  public void testEqualsAndHashCode(String description, Filter[] filters1, Filter[] filters2) {
    DeltaV2ScanBuilder builder1 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    if (filters1.length > 0) {
      pushFilters(builder1, filters1);
    }
    Batch batch1 = asDeltaV2Scan(builder1.build()).toBatch();

    DeltaV2ScanBuilder builder2 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    if (filters2.length > 0) {
      pushFilters(builder2, filters2);
    }
    Batch batch2 = asDeltaV2Scan(builder2.build()).toBatch();

    assertEquals(batch1, batch2);
    assertEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentPushedFilters() {
    DeltaV2ScanBuilder builder1 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    Batch batch1 = asDeltaV2Scan(builder1.build()).toBatch();

    DeltaV2ScanBuilder builder2 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    pushFilters(builder2, new EqualTo("city", "hz"));
    Batch batch2 = asDeltaV2Scan(builder2.build()).toBatch();

    assertNotEquals(batch1, batch2);
    assertNotEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testBatchReadPreservesPercentLiteralStringPartitionValue(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, p STRING) USING delta PARTITIONED BY (p)", path));
    // Each value is chosen so an erroneous second unescape would produce a result that's
    // pairwise distinct from the input AND from the other cases' bug results, so a regression
    // cannot coincidentally agree with the expected value:
    //   "%20"   -> bug result " "   (canonical space-collapse)
    //   "%25"   -> bug result "%"   (self-encoding of `%`)
    //   "a%2Fb" -> bug result "a/b" (embedded percent escape mid-string)
    spark.sql(
        String.format(
            "INSERT INTO delta.`%s` VALUES (1, '%%20'), (2, '%%25'), (3, 'a%%2Fb')", path));

    List<Row> rows =
        spark
            .sql(String.format("SELECT id, p FROM dsv2.delta.`%s` ORDER BY id", path))
            .collectAsList();

    assertEquals(3, rows.size());
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals("%20", rows.get(0).getString(1));
    assertEquals(2L, rows.get(1).getLong(0));
    assertEquals("%25", rows.get(1).getString(1));
    assertEquals(3L, rows.get(2).getLong(0));
    assertEquals("a%2Fb", rows.get(2).getString(1));
  }
}
