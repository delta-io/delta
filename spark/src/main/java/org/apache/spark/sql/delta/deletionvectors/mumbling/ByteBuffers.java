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

package org.apache.spark.sql.delta.deletionvectors.mumbling;

import java.nio.ByteBuffer;

/**
 * Absolute single-byte accessors for a {@link ByteBuffer}, indexed relative to the buffer's
 * current position and leaving the buffer's position and limit unmodified.
 */
final class ByteBuffers {

  private ByteBuffers() {}

  /**
   * Reads the unsigned byte at {@code buffer.position() + offset}.
   *
   * @param buffer the buffer to read from; position and limit are not modified
   * @param offset the offset relative to the buffer's position
   * @return the byte value in the range {@code [0, 255]}
   */
  static int readByte(ByteBuffer buffer, int offset) {
    return buffer.get(buffer.position() + offset) & 0xFF;
  }

  /**
   * Writes the low 8 bits of {@code value} at {@code buffer.position() + offset}.
   *
   * @param buffer the buffer to write to; position and limit are not modified
   * @param value the value whose low byte is written
   * @param offset the offset relative to the buffer's position
   */
  static void writeByte(ByteBuffer buffer, int value, int offset) {
    buffer.put(buffer.position() + offset, (byte) value);
  }
}
