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

package io.delta.flink.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.flink.TestHelper;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.checksum.ChecksumReader;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.metrics.FileSizeHistogramResult;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.File;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Checksums must agree with their own file count and size even after a failed commit attempt left
 * the cached snapshot's histogram skewed, or when the last checksum on disk is already skewed.
 */
class ChecksumListenerTest extends TestHelper {

  private static final StructType SCHEMA = new StructType().add("id", IntegerType.INTEGER);

  /** The connector retries an expired-credential failure from the same cached snapshot. */
  @Test
  void checksumStaysConsistentAfterCredentialExpiredRetry() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          try {
            commitPlain(table, "app", 1);
            waitForChecksum(dir, 1);

            AtomicInteger iterations = new AtomicInteger();
            table.commit(
                failOnFirstIteration(
                    iterations,
                    () ->
                        new UncheckedIOException(
                            new AccessDeniedException("injected: expired credential"))),
                "app",
                2,
                Collections.emptyMap());
            assertEquals(2, iterations.get(), "expected exactly one retry");

            assertChecksumConsistent(dir, 2);
          } finally {
            table.close();
          }
        });
  }

  /**
   * A failure escapes to a task restart and the committable is replayed from the cached snapshot.
   */
  @Test
  void checksumStaysConsistentAfterFailedAttemptEscapes() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir);
          try {
            commitPlain(table, "app", 1);
            waitForChecksum(dir, 1);

            AtomicInteger iterations = new AtomicInteger();
            assertThrows(
                RuntimeException.class,
                () ->
                    table.commit(
                        failOnFirstIteration(
                            iterations, () -> new IllegalStateException("injected: task failure")),
                        "app",
                        2,
                        Collections.emptyMap()));
            assertEquals(1, iterations.get(), "a non-retryable failure must not be retried");

            // The replayed committable after the restart.
            commitPlain(table, "app", 2);
            assertChecksumConsistent(dir, 2);
          } finally {
            table.close();
          }
        });
  }

  /**
   * The latest .crc is already skewed (SIMPLE path). Row tracking keeps domain metadata in the
   * checksum; without it the Kernel escalates to FULL and the test would pass for the wrong reason.
   */
  @Test
  void nextCommitRepairsAnAlreadySkewedChecksum() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, Map.of("delta.enableRowTracking", "true"));
          try {
            commitPlain(table, "app", 1);
            waitForChecksum(dir, 1);
            skewHistogram(checksumFile(dir, 1));
            CRCInfo skewed = readChecksum(dir, 1);
            assertTrue(skewed.getDomainMetadata().isPresent(), "test table lacks domain metadata");
            assertTrue(
                histogramFileCount(skewed) == skewed.getNumFiles() + 1, "skew was not applied");
            // Drop the in-memory snapshot so the next commit reads version 1's skewed .crc.
            table.reloadSnapshot();

            commitPlain(table, "app", 2);
            assertChecksumConsistent(dir, 2);
            // Otherwise every following commit builds on the skewed in-memory CRC again.
            assertCachedSnapshotConsistent(table);
          } finally {
            table.close();
          }
        });
  }

  /** The latest .crc is missing over a skewed complete one, so the write goes through FULL mode. */
  @Test
  void fullModeChecksumDoesNotInheritASkewedBase() {
    withTempDir(
        dir -> {
          HadoopTable table = openTable(dir, Map.of("delta.enableRowTracking", "true"));
          try {
            commitPlain(table, "app", 1);
            waitForChecksum(dir, 1);
            commitPlain(table, "app", 2);
            waitForChecksum(dir, 2);
            skewHistogram(checksumFile(dir, 1));
            assertTrue(checksumFile(dir, 2).delete(), "could not drop version 2's checksum");
            table.reloadSnapshot();

            commitPlain(table, "app", 3);
            assertChecksumConsistent(dir, 3);
          } finally {
            table.close();
          }
        });
  }

  private static void assertCachedSnapshotConsistent(HadoopTable table)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10_000;
    String last = "no snapshot";
    while (System.currentTimeMillis() < deadline) {
      CRCInfo crc =
          ((io.delta.kernel.internal.SnapshotImpl) table.snapshot().get())
              .getCurrentCrcInfo()
              .orElseThrow();
      if (histogramFileCount(crc) == crc.getNumFiles()) {
        return;
      }
      last = histogramFileCount(crc) + " vs " + crc.getNumFiles();
      Thread.sleep(100);
    }
    throw new AssertionError("cached snapshot keeps the skewed checksum: " + last);
  }

  private HadoopTable openTable(File dir) {
    return openTable(dir, Collections.emptyMap());
  }

  private HadoopTable openTable(File dir, Map<String, String> conf) {
    HadoopTable table = new HadoopTable(URI.create(dir.getAbsolutePath()), conf, SCHEMA, List.of());
    table.open();
    return table;
  }

  /** Counts the first non-empty bin's average file once more: one file counted twice. */
  private static void skewHistogram(File crcFile) throws java.io.IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode crc = (ObjectNode) mapper.readTree(crcFile);
    ObjectNode histogram = (ObjectNode) crc.get("fileSizeHistogram");
    ArrayNode counts = (ArrayNode) histogram.get("fileCounts");
    ArrayNode bytes = (ArrayNode) histogram.get("totalBytes");
    for (int i = 0; i < counts.size(); i++) {
      long count = counts.get(i).asLong();
      if (count > 0) {
        counts.set(i, count + 1);
        bytes.set(i, bytes.get(i).asLong() + bytes.get(i).asLong() / count);
        mapper.writeValue(crcFile, crc);
        // The local FileSystem verifies its own ".<name>.crc" sidecar; it no longer matches.
        new File(crcFile.getParentFile(), "." + crcFile.getName() + ".crc").delete();
        return;
      }
    }
    throw new IllegalStateException("no non-empty histogram bin in " + crcFile);
  }

  private void commitPlain(HadoopTable table, String appId, long txnId) {
    List<Row> actions = dummyAddFileRows(SCHEMA, 3, i -> Collections.emptyMap());
    table.commit(iterable(() -> actions.iterator()), appId, txnId, Collections.emptyMap());
  }

  /** Yields one add action and then throws, on the first pass only. */
  private CloseableIterable<Row> failOnFirstIteration(
      AtomicInteger iterations, Supplier<RuntimeException> failure) {
    Row add = dummyAddFileRow(SCHEMA, 10, Collections.emptyMap());
    return iterable(
        () -> {
          int pass = iterations.incrementAndGet();
          java.util.Iterator<Row> normal = List.of(add).iterator();
          if (pass > 1) {
            return normal;
          }
          return new java.util.Iterator<Row>() {
            boolean served = false;

            @Override
            public boolean hasNext() {
              if (served) {
                throw failure.get();
              }
              return true;
            }

            @Override
            public Row next() {
              served = true;
              return add;
            }
          };
        });
  }

  private static CloseableIterable<Row> iterable(Supplier<java.util.Iterator<Row>> source) {
    return new CloseableIterable<Row>() {
      @Override
      public CloseableIterator<Row> iterator() {
        return Utils.toCloseableIterator(source.get());
      }

      @Override
      public void close() {}
    };
  }

  private static File checksumFile(File dir, long version) {
    return new File(new File(dir, "_delta_log"), String.format("%020d.crc", version));
  }

  private static void waitForChecksum(File dir, long version) throws InterruptedException {
    File crc = checksumFile(dir, version);
    // The local FileSystem renames the file, then its sidecar; wait for both before touching it.
    File sidecar = new File(crc.getParentFile(), "." + crc.getName() + ".crc");
    long deadline = System.currentTimeMillis() + 30_000;
    while (!(crc.exists() && sidecar.exists()) && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
    assertTrue(
        crc.exists(),
        "checksum for version "
            + version
            + " was not written; _delta_log="
            + Arrays.toString(new File(dir, "_delta_log").list()));
  }

  private static void assertChecksumConsistent(File dir, long version) throws Exception {
    waitForChecksum(dir, version);
    CRCInfo crc = readChecksum(dir, version);
    FileSizeHistogramResult h = crc.getFileSizeHistogram().get().captureFileSizeHistogramResult();
    assertEquals(
        crc.getNumFiles(), histogramFileCount(crc), "histogram file count disagrees with numFiles");
    assertEquals(
        crc.getTableSizeBytes(),
        Arrays.stream(h.getTotalBytes()).sum(),
        "histogram byte total disagrees with tableSizeBytes");
  }

  private static CRCInfo readChecksum(File dir, long version) {
    Engine engine = DefaultEngine.create(new Configuration());
    File crcFile = checksumFile(dir, version);
    Optional<CRCInfo> crcOpt =
        ChecksumReader.tryReadChecksumFile(
            engine, FileStatus.of(crcFile.getAbsolutePath(), crcFile.length(), 0));
    assertTrue(crcOpt.isPresent(), "checksum unreadable");
    assertTrue(crcOpt.get().getFileSizeHistogram().isPresent(), "checksum lost its histogram");
    return crcOpt.get();
  }

  private static long histogramFileCount(CRCInfo crc) {
    return Arrays.stream(
            crc.getFileSizeHistogram().get().captureFileSizeHistogramResult().getFileCounts())
        .sum();
  }
}
