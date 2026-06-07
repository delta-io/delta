/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.kernel.dv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.flink.TestHelper;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.NoSuchFileException;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Round-trip + edge-case tests for {@link BinDVAccess}.
 *
 * <p>{@link BinDVAccess} no longer carries its own Hadoop {@code Configuration}; the {@link Engine}
 * we pass in wires up the Hadoop-backed {@code FileIO} for both write and read paths.
 */
class BinDVAccessTest extends TestHelper {

  @Test
  void testRoundTripEmptyBitmap() {
    withTempDir(
        dir -> {
          BinDVAccess store = new BinDVAccess();
          Engine engine = DefaultEngine.create(new Configuration());
          String filePath = pathIn(dir, "empty.bin");

          RoaringBitmapArray original = new RoaringBitmapArray();
          store.write(engine, filePath, original);

          RoaringBitmapArray restored = store.read(engine, filePath);
          assertArrayEquals(original.toArray(), restored.toArray());
        });
  }

  @Test
  void testRoundTripDenseLowKeyBitmap() {
    withTempDir(
        dir -> {
          BinDVAccess store = new BinDVAccess();
          Engine engine = DefaultEngine.create(new Configuration());
          String filePath = pathIn(dir, "dense.bin");

          // Single high-32-bit key (= 0); covers the dominant DV case where row ordinals fit in a
          // single 32-bit RoaringBitmap.
          RoaringBitmapArray original = new RoaringBitmapArray();
          for (long v = 0; v < 100_000; v += 7) {
            original.add(v);
          }
          store.write(engine, filePath, original);

          RoaringBitmapArray restored = store.read(engine, filePath);
          assertArrayEquals(original.toArray(), restored.toArray());
        });
  }

  @Test
  void testRoundTripSparseAcrossHighKeys() {
    withTempDir(
        dir -> {
          BinDVAccess store = new BinDVAccess();
          Engine engine = DefaultEngine.create(new Configuration());
          String filePath = pathIn(dir, "sparse.bin");

          // Values across several distinct high-32-bit keys; exercises the per-key encoding loop.
          RoaringBitmapArray original =
              RoaringBitmapArray.create(
                  0L, 7L, (1L << 32) + 13L, (5L << 32), (5L << 32) + 1L, (100L << 32) + 999_999L);
          store.write(engine, filePath, original);

          RoaringBitmapArray restored = store.read(engine, filePath);
          assertArrayEquals(original.toArray(), restored.toArray());
        });
  }

  @Test
  void testReadMissingFileThrows() {
    withTempDir(
        dir -> {
          BinDVAccess store = new BinDVAccess();
          Engine engine = DefaultEngine.create(new Configuration());
          String filePath = pathIn(dir, "does-not-exist.bin");

          // FileIO.getFileStatus raises FileNotFoundException; HadoopFileIO's underlying
          // FileSystem may surface it as java.nio.file.NoSuchFileException OR
          // java.io.FileNotFoundException depending on the FS impl, so we walk the cause chain
          // for either marker.
          RuntimeException ex =
              assertThrows(RuntimeException.class, () -> store.read(engine, filePath));
          assertCauseMatches(
              ex,
              t -> t instanceof NoSuchFileException || t instanceof java.io.FileNotFoundException);
        });
  }

  @Test
  void testReadDetectsCorruptedChecksum() {
    withTempDir(
        dir -> {
          BinDVAccess store = new BinDVAccess();
          Engine engine = DefaultEngine.create(new Configuration());
          String filePath = pathIn(dir, "corrupt.bin");

          RoaringBitmapArray bitmap = RoaringBitmapArray.create(10L, 20L, 30L);
          store.write(engine, filePath, bitmap);

          // Flip the last byte (part of the CRC32 trailer); the reader must reject the file.
          File diskFile = new File(dir, "corrupt.bin");
          try (RandomAccessFile raf = new RandomAccessFile(diskFile, "rw")) {
            long size = raf.length();
            raf.seek(size - 1);
            int b = raf.read();
            raf.seek(size - 1);
            raf.write(b ^ 0x01);
          }

          RuntimeException ex =
              assertThrows(RuntimeException.class, () -> store.read(engine, filePath));
          assertContainsIgnoringWrappers(ex, "checksum");
        });
  }

  @Test
  void testReadDetectsBadVersionByte() {
    withTempDir(
        dir -> {
          BinDVAccess store = new BinDVAccess();
          Engine engine = DefaultEngine.create(new Configuration());
          String filePath = pathIn(dir, "badversion.bin");

          store.write(engine, filePath, RoaringBitmapArray.create(1L));

          File diskFile = new File(dir, "badversion.bin");
          try (RandomAccessFile raf = new RandomAccessFile(diskFile, "rw")) {
            raf.seek(0);
            raf.write(0x09);
          }

          RuntimeException ex =
              assertThrows(RuntimeException.class, () -> store.read(engine, filePath));
          assertContainsIgnoringWrappers(ex, "version");
        });
  }

  @Test
  void testWriteUsesPortableMagic() {
    // The serialized payload must start with the Portable magic number, so we catch accidental
    // format drift away from Spark/Kernel interop.
    byte[] payload = RoaringBitmapArray.create(1L, 2L, 3L).serializeAsByteArray();
    int magic =
        (payload[0] & 0xFF)
            | ((payload[1] & 0xFF) << 8)
            | ((payload[2] & 0xFF) << 16)
            | ((payload[3] & 0xFF) << 24);
    assertEquals(RoaringBitmapArray.PORTABLE_MAGIC_NUMBER, magic);
  }

  // ----------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------

  private static String pathIn(File dir, String name) {
    return new File(dir, name).toURI().toString();
  }

  /** Walk an exception's cause chain looking for one that matches {@code predicate}. */
  private static void assertCauseMatches(
      Throwable actual, java.util.function.Predicate<Throwable> predicate) {
    Throwable cur = actual;
    while (cur != null) {
      if (predicate.test(cur)) {
        return;
      }
      cur = cur.getCause();
    }
    throw new AssertionError("No matching cause in chain", actual);
  }

  private static void assertContainsIgnoringWrappers(Throwable actual, String needle) {
    Throwable cur = actual;
    while (cur != null) {
      String msg = cur.getMessage();
      if (msg != null && msg.toLowerCase().contains(needle.toLowerCase())) {
        return;
      }
      cur = cur.getCause();
    }
    throw new AssertionError(
        "Expected exception message containing '" + needle + "' but got chain: " + actual);
  }
}
