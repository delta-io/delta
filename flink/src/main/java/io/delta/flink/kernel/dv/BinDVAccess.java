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

import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Ported from {@code org.apache.spark.sql.delta.storage.dv.DeletionVectorStore}; layout {@code
 * [version][length BE][payload][crc32 BE]}. Supports a single DV per bin file: {@link
 * #write}/{@link #store} emit one DV at offset 1 and {@link #read} rejects any larger file rather
 * than mis-reading it.
 *
 * <p>Multi-DV-per-file support (needed to modify DVs in place) is future work.
 */
public class BinDVAccess extends DVAccess {

  static final byte DV_FILE_FORMAT_VERSION_ID_V1 = 1;

  /**
   * Offset of the DV payload's length-prefix within the {@code .bin} file -- 1 byte past the file
   * header. {@link DeletionVectorDescriptor#getOffset()} stores this value so readers know where to
   * start.
   */
  static final int DV_PAYLOAD_OFFSET_IN_FILE = 1;

  /** Filename pattern for UUID-keyed DV files; distinct from Spark. */
  private static final String DV_FILE_NAME_TEMPLATE = "dv_flink_%s.bin";

  @Override
  public void write(Engine engine, String filePath, RoaringBitmapArray bitmap) {
    Objects.requireNonNull(filePath, "filePath is null");
    Objects.requireNonNull(bitmap, "bitmap is null");
    writePayload(engine, filePath, bitmap.serializeAsByteArray());
  }

  /**
   * Writes the bitmap to a fresh UUID-named {@code .bin} file under {@code tableRootPath} and emits
   * a <b>path-based</b> ({@code "p"}) descriptor whose {@code pathOrInlineDv} is the absolute URI
   * of the file. We avoid the UUID ({@code "u"}) variant for new writes -- path descriptors
   * round-trip without any UUID encoding/prefix handling on the reader side -- but {@link #read}
   * still accepts UUID descriptors via {@link DeletionVectorDescriptor#getAbsolutePath} for interop
   * with files written by other engines.
   */
  @Override
  public DeletionVectorDescriptor store(
      Engine engine, String tableRootPath, RoaringBitmapArray bitmap) {
    Objects.requireNonNull(tableRootPath, "tableRootPath is null");
    Objects.requireNonNull(bitmap, "bitmap is null");

    String filePath =
        joinPath(tableRootPath, String.format(DV_FILE_NAME_TEMPLATE, UUID.randomUUID()));
    // Serialize exactly once -- both the file write and the descriptor's sizeInBytes need the
    // payload, and we want them guaranteed-consistent.
    byte[] payload = bitmap.serializeAsByteArray();
    writePayload(engine, filePath, payload);

    return new DeletionVectorDescriptor(
        DeletionVectorDescriptor.PATH_DV_MARKER,
        filePath,
        Optional.of(DV_PAYLOAD_OFFSET_IN_FILE),
        payload.length,
        bitmap.cardinality());
  }

  /**
   * Reads the single deletion vector from {@code filePath}, starting at the beginning of the file.
   *
   * <p>Only single-DV files are supported: the file must consist of exactly {@code [version][length
   * BE][payload][crc32 BE]} and nothing more. If the file is longer than one DV (e.g. a delta-spark
   * bin-packed file holding multiple DVs), this throws rather than returning a truncated or wrong
   * result. See the class Javadoc for the multi-DV limitation.
   */
  @Override
  public RoaringBitmapArray read(Engine engine, String filePath) {
    Objects.requireNonNull(filePath, "filePath is null");
    FileIO fileIO = fileIOFromEngine(engine);
    try {
      long size = fileIO.getFileStatus(filePath).getSize();
      InputFile inputFile = fileIO.newInputFile(filePath, size);
      try (SeekableInputStream rawIn = inputFile.newStream();
          DataInputStream in = new DataInputStream(rawIn)) {
        byte version = in.readByte();
        if (version != DV_FILE_FORMAT_VERSION_ID_V1) {
          throw new IOException(
              "Unexpected DV file format version "
                  + version
                  + " (expected "
                  + DV_FILE_FORMAT_VERSION_ID_V1
                  + ") in "
                  + filePath);
        }
        int length = in.readInt();
        if (length < 0) {
          throw new IOException("Negative DV payload length " + length + " in " + filePath);
        }
        // Enforce the single-DV-per-file contract: a lone DV occupies exactly
        // [1 version][4 length][length payload][4 crc] bytes. A larger file means it holds more
        // than one DV (e.g. a delta-spark bin-packed file), which this reader does not support --
        // fail loudly instead of silently returning only the first DV.
        long expectedSize = 1L /* version */ + 4L /* length */ + (long) length + 4L /* crc */;
        if (size != expectedSize) {
          throw new IOException(
              "BinDVAccess supports only single-DV files, but "
                  + filePath
                  + " has size "
                  + size
                  + " (expected "
                  + expectedSize
                  + " for a single DV with payload length "
                  + length
                  + "); bin-packed multi-DV files are not supported");
        }
        byte[] data = new byte[length];
        in.readFully(data);
        int storedChecksum = in.readInt();
        int computedChecksum = crc32(data);
        if (storedChecksum != computedChecksum) {
          throw new IOException(
              "DV checksum mismatch in "
                  + filePath
                  + " (stored="
                  + storedChecksum
                  + ", computed="
                  + computedChecksum
                  + ")");
        }
        return RoaringBitmapArray.readFrom(data);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read DV file: " + filePath, e);
    }
  }

  /**
   * Write {@code payload} (a pre-serialized {@link RoaringBitmapArray}) to {@code filePath} wrapped
   * in the BIN framing. Shared by {@link #write} and {@link #store} so the file layout lives in
   * exactly one place.
   */
  private static void writePayload(Engine engine, String filePath, byte[] payload) {
    int checksum = crc32(payload);
    FileIO fileIO = fileIOFromEngine(engine);
    OutputFile outputFile = fileIO.newOutputFile(filePath);
    // Only the DataOutputStream goes in try-with-resources. Hadoop's putIfAbsent OutputFile wraps
    // a temp-write + rename-on-close pattern that is NOT idempotent -- closing the
    // PositionOutputStream a second time would re-attempt the rename and fail.
    try (DataOutputStream out = new DataOutputStream(outputFile.create(/* putIfAbsent */ true))) {
      out.writeByte(DV_FILE_FORMAT_VERSION_ID_V1);
      out.writeInt(payload.length);
      out.write(payload);
      out.writeInt(checksum);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write DV file: " + filePath, e);
    }
  }

  private static String joinPath(String base, String name) {
    return base.endsWith("/") ? base + name : base + "/" + name;
  }
}
