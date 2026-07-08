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

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import io.delta.tables.DeltaTable;
import io.sparkuctest.mock.CredentialTestFileSystem;
import io.sparkuctest.mock.TimeBasedCredGenerator;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.util.SerializableConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies cloud-vendor storage-credential renewal end to end.
 *
 * <p>A time-based credential generator is injected into the embedded UC server, issuing credentials
 * that are each valid for {@link #DEFAULT_DURATION_MILLIS}. A Spark job uses {@link
 * CredentialTestFileSystem} to assert that the credential the connector resolves is currently valid
 * and to count renewals; advancing the shared in-JVM manual clock ({@link #testClock()}) past a
 * credential's expiry rotates the issued credentials and forces the connector to renew. Determinism
 * relies on the connector resolving the same named manual clock via {@code UC_TEST_CLOCK_NAME}.
 *
 * <p>Reuses {@link UCDeltaTableIntegrationBaseTest}'s embedded server + Spark session plumbing,
 * overriding {@link #sparkWorkerThreads()} (single task, so renewals are counted deterministically)
 * and {@link #extraSparkConf()} (the manual clock and per-cloud filesystem wiring).
 *
 * <p>Local-only: needs the embedded server and the in-JVM manual clock. Each concrete subclass
 * carries {@code @DisabledIf("isUCRemoteConfigured")} so it is skipped when {@code UC_REMOTE=true}
 * (a class-level disable condition on an abstract base is not inherited by subclasses).
 */
public abstract class BaseCredRenewIT extends UCDeltaTableIntegrationBaseTest {
  protected static final String BUCKET_NAME = CredentialTestFileSystem.BUCKET_NAME;
  protected static final long DEFAULT_DURATION_MILLIS =
      CredentialTestFileSystem.DEFAULT_DURATION_MILLIS;
  private static final String CLOCK_NAME = UUID.randomUUID().toString();
  // A few rounds is enough to prove the renewal count keeps advancing across expiries.
  private static final int RENEWAL_ROUNDS = 3;

  /** Cloud scheme under test, e.g. {@code s3}. */
  protected abstract String scheme();

  /** Cloud-specific Hadoop filesystem wiring, i.e. the {@code fs.<scheme>.impl} test filesystem. */
  protected abstract Map<String, String> hadoopFsProps();

  /**
   * A table LOCATION named {@code name} under the shared local-server base directory, composed the
   * same way as {@link UnityCatalogInfo#baseTableLocation()} (fake bucket + base dir) but under
   * this suite's cloud scheme so the path routes to its {@code fs.<scheme>.impl} test filesystem.
   * These suites are local-only (see class doc), so relying on the local base dir is safe.
   */
  private String location(String name) {
    return scheme() + "://" + BUCKET_NAME + baseTableDir() + "/" + name;
  }

  @Override
  protected int sparkWorkerThreads() {
    return 1; // single-threaded so renewals are counted in one task
  }

  @Override
  protected Map<String, String> extraSparkConf() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.hadoop." + UCHadoopConfConstants.UC_TEST_CLOCK_NAME, CLOCK_NAME);
    // No early-renewal slack: renew exactly when the current credential expires, so renewals line
    // up with the clock-driven expiry boundaries the assertions check.
    conf.put("spark.hadoop." + UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, "0");
    // credScopedFs.enabled is left to the connector default; the test tolerates either filesystem
    // (see the CredScopedFileSystem unwrap in testFileSystemRenewal). The connector also disables
    // the FS cache and sets the vended bucket, so neither is configured here.
    // Per-cloud filesystem wiring (fs.<scheme>.impl); applied after the UC defaults.
    conf.putAll(hadoopFsProps());
    return conf;
  }

  @AfterEach
  public void dropTestTables() {
    sql("DROP TABLE IF EXISTS %s", fullTableName("demo"));
    sql("DROP TABLE IF EXISTS %s", fullTableName("src"));
    sql("DROP TABLE IF EXISTS %s", fullTableName("dst"));
  }

  @BeforeAll
  public void useManualClockGenerator() {
    // Point the server-side credential generator at this suite's manual clock + short duration, so
    // credentials rotate deterministically as the test advances the clock. Restored in @AfterAll.
    TimeBasedCredGenerator.useManualClock(CLOCK_NAME, DEFAULT_DURATION_MILLIS);
  }

  @AfterAll
  public void removeManualClock() {
    TimeBasedCredGenerator.useSystemClock();
    Clock.removeManualClock(CLOCK_NAME);
  }

  /**
   * The shared in-JVM manual clock; the connector and the server-side generator both resolve it.
   */
  private static Clock testClock() {
    return Clock.getManualClock(CLOCK_NAME);
  }

  @Test
  public void testFileSystemRenewal() throws Exception {
    String table = fullTableName("demo");
    String location = location("fs");
    Path locPath = new Path(location);

    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", table, location);
    sql("INSERT INTO %s VALUES (1)", table);

    // Table-level Hadoop conf carries the connector's vended-credential wiring; we reuse it inside
    // a Spark task to drive filesystem access and measure renewals.
    SerializableConfiguration serialConf =
        new SerializableConfiguration(
            DeltaTable.forName(spark(), table).deltaLog().newDeltaHadoopConf());

    List<Row> rows =
        spark()
            .read()
            .format("delta")
            .table(table)
            .toJavaRDD()
            .map(
                row -> {
                  Configuration conf = serialConf.value();
                  FileSystem rawFs = FileSystem.get(new URI(location), conf);
                  // When credScopedFs is enabled, unwrap one level to reach the real delegate; the
                  // original impl is restored under fs.<scheme>.impl.original, so the delegate is
                  // never itself a CredScopedFileSystem.
                  CredentialTestFileSystem<?> fs =
                      (CredentialTestFileSystem<?>)
                          (rawFs instanceof CredScopedFileSystem
                              ? ((CredScopedFileSystem) rawFs).getRawFileSystem()
                              : rawFs);

                  // Baseline the renewal count and assert each clock advance increments it by
                  // exactly one, rather than assuming it starts at zero -- robust to any renewals
                  // already recorded on this FS instance before the loop.
                  fs.getFileStatus(locPath);
                  int baseline = fs.renewalCount();

                  for (int refreshIndex = 0; refreshIndex < RENEWAL_ROUNDS; refreshIndex += 1) {
                    // Pre-check before the credential renewal.
                    fs.getFileStatus(locPath);
                    assertThat(fs.renewalCount()).isEqualTo(baseline + refreshIndex);

                    // Advance the clock to trigger the renewal.
                    testClock().sleep(Duration.ofMillis(DEFAULT_DURATION_MILLIS));

                    // Post-check after the credential renewal.
                    fs.getFileStatus(locPath);
                    assertThat(fs.renewalCount()).isEqualTo(baseline + refreshIndex + 1);
                  }

                  // Return the number of renewals actually observed so the assertion below confirms
                  // the single task ran to completion through every round.
                  return RowFactory.create(fs.renewalCount() - baseline);
                })
            .collect();

    assertThat(rows.stream().map(r -> r.getInt(0)).collect(Collectors.toList()))
        .isEqualTo(List.of(RENEWAL_ROUNDS));
  }

  @Test
  public void testDeltaReadWriteRenewal() throws Exception {
    String srcTable = fullTableName("src");
    String dstTable = fullTableName("dst");
    String srcLoc = location("src");
    String dstLoc = location("dst");

    sql(
        "CREATE TABLE %s (id INT) USING delta LOCATION '%s' PARTITIONED BY (partition INT)",
        srcTable, srcLoc);
    sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", dstTable, dstLoc);
    sql("INSERT INTO %s VALUES (1, 1), (2, 2), (3, 3)", srcTable);

    // Each partition becomes a task; with parallelism 1 they run sequentially, and the accumulated
    // clock advance triggers credential renewal. The job succeeds only because renewal is enabled.
    spark()
        .read()
        .format("delta")
        .table(srcTable)
        .mapPartitions(
            (MapPartitionsFunction<Row, Integer>)
                input -> {
                  testClock().sleep(Duration.ofMillis(DEFAULT_DURATION_MILLIS));
                  List<Integer> out = new ArrayList<>();
                  while (input.hasNext()) {
                    out.add(input.next().getInt(0));
                  }
                  return out.iterator();
                },
            Encoders.INT())
        .withColumnRenamed("value", "id")
        .write()
        .format("delta")
        .mode("append")
        .saveAsTable(dstTable);

    check(dstTable, List.of(row("1"), row("2"), row("3")));
  }
}
