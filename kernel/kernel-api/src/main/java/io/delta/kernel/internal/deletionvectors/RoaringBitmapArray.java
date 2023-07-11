/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.deletionvectors;

import io.delta.kernel.utils.Tuple2;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

import static io.delta.kernel.internal.util.InternalUtils.checkArgument;

// TODO: add test suite
// If we implement additional methods (i.e. serialize) we can copy the test suite from delta-spark
/**
 * A 64-bit extension of [[RoaringBitmap]] that is optimized for cases that usually fit within
 * a 32-bit bitmap, but may run over by a few bits on occasion.
 *
 * This focus makes it different from [[org.roaringbitmap.longlong.Roaring64NavigableMap]] and
 * [[org.roaringbitmap.longlong.Roaring64Bitmap]] which focus on sparse bitmaps over the whole
 * 64-bit range.
 *
 * Structurally, this implementation simply uses the most-significant 4 bytes to index into
 * an array of 32-bit [[RoaringBitmap]] instances.
 * The array is grown as necessary to accommodate the largest value in the bitmap.
 *
 * *Note:* As opposed to the other two 64-bit bitmap implementations mentioned above,
 *         this implementation cannot accommodate `Long` values where the most significant
 *         bit is non-zero (i.e., negative `Long` values).
 *         It cannot even accommodate values where the 4 high-order bytes are `Int.MaxValue`,
 *         because then the length of the `bitmaps` array would be a negative number
 *         (`Int.MaxValue + 1`).
 *
 * Taken from https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/deletionvectors/RoaringBitmapArray.scala
 */
final public class RoaringBitmapArray {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    /** The largest value a [[RoaringBitmapArray]] can possibly represent. */
    static final long MAX_REPRESENTABLE_VALUE = composeFromHighLowBytes(
            Integer.MAX_VALUE - 1, Integer.MIN_VALUE);

    /**
     * @param value Any `Long`; positive or negative.
     * @return An `Int` holding the 4 high-order bytes of information of the input `value`.
     */
    static int highBytes(long value) {
        return (int) (value >> 32);
    }

    /**
     * @param value Any `Long`; positive or negative.
     * @return An `Int` holding the 4 low-order bytes of information of the input `value`.
     */
    static int lowBytes(long value) {
        return (int) value;
    }

    /**
     * Combine high and low 4 bytes of a pair of `Int`s into a `Long`.
     *
     * This is essentially the inverse of [[decomposeHighLowBytes()]].
     *
     * @param high An `Int` representing the 4 high-order bytes of the output `Long`
     * @param low An `Int` representing the 4 low-order bytes of the output `Long`
     * @return A `Long` composing the `high` and `low` bytes.
     */
    static long composeFromHighLowBytes(int high, int low) {
        // Must bitmask to avoid sign extension.
        return (((long)high) << 32) | (((long) low) & 0xFFFFFFFFL);
    }

    /** Deserialize the right instance from the given bytes */
    static RoaringBitmapArray readFrom(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        RoaringBitmapArray bitmap = new RoaringBitmapArray();
        bitmap.deserialize(buffer);
        return bitmap;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private RoaringBitmap[] bitmaps;

    /**
     * Deserialize the contents of `buffer` into this [[RoaringBitmapArray]].
     *
     * All existing content will be discarded!
     *
     * == Serialization Format ==
     * - A Magic Number indicating the format used (4 bytes)
     * - The actual data as specified by the format.
     */
    void deserialize(ByteBuffer buffer) throws IOException {
        checkArgument(ByteOrder.LITTLE_ENDIAN == buffer.order(),
                "RoaringBitmapArray has to be deserialized using a little endian buffer");

        int magicNumber = buffer.getInt();
        if (magicNumber == NativeRoaringBitmapArraySerializationFormat.MAGIC_NUMBER) {
            bitmaps = NativeRoaringBitmapArraySerializationFormat.deserialize(buffer);
        } else if (magicNumber == PortableRoaringBitmapArraySerializationFormat.MAGIC_NUMBER) {
            bitmaps = PortableRoaringBitmapArraySerializationFormat.deserialize(buffer);
        } else {
            throw new IOException("Unexpected RoaringBitmapArray magic number " + magicNumber);
        }
    }

    /**
     * Checks whether the value is included,
     * which is equivalent to checking if the corresponding bit is set.
     */
    public boolean contains(long value) {
        checkArgument(value >= 0 && value <= MAX_REPRESENTABLE_VALUE);
        int high = highBytes(value);
        if (high >= bitmaps.length) {
            return false;
        } else {
            RoaringBitmap highBitmap = bitmaps[high];
            int low = lowBytes(value);
            return highBitmap.contains(low);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Serialization Formats
    ////////////////////////////////////////////////////////////////////////////////

    /**
     * == Serialization Format ==
     * - Number of bitmaps (4 bytes)
     * - For each individual bitmap:
     *    - Length of the serialized bitmap
     *    - Serialized bitmap data using the standard format
     *      (see https://github.com/RoaringBitmap/RoaringFormatSpec)
     */
    static class NativeRoaringBitmapArraySerializationFormat {
        /** Magic number prefix for serialization with this format. */
        static int MAGIC_NUMBER = 1681511376;

        /** Deserialize all bitmaps from the `buffer` into a fresh array. */
        static RoaringBitmap[] deserialize(ByteBuffer buffer) throws IOException {
            int numberOfBitmaps = buffer.getInt();
            if (numberOfBitmaps < 0) {
                throw new IOException(String.format(
                        "Invalid RoaringBitmapArray length (%s < 0)", numberOfBitmaps));
            }
            RoaringBitmap[] bitmaps = new RoaringBitmap[numberOfBitmaps];
            for (int i = 0; i < numberOfBitmaps; i++) {
                bitmaps[i] = new RoaringBitmap();
                int bitmapSize = buffer.getInt();
                bitmaps[i].deserialize(buffer);
                // RoaringBitmap.deserialize doesn't move the buffer's pointer
                buffer.position(buffer.position() + bitmapSize);
            }
            return bitmaps;
        }
    }

    /**
     * This is the "official" portable format defined in the spec.
     *
     * See [[https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations]]
     *
     * == Serialization Format ==
     *   - Number of bitmaps (8 bytes, upper 4 are basically padding)
     *   - For each individual bitmap, in increasing key order (unsigned, technically, but
     *     RoaringBitmapArray doesn't support negative keys anyway.):
     *     - key of the bitmap (upper 32 bit)
     *     - Serialized bitmap data using the standard format (see
     *       https://github.com/RoaringBitmap/RoaringFormatSpec)
     */
    static class PortableRoaringBitmapArraySerializationFormat {
        /** Magic number prefix for serialization with this format. */
        static int MAGIC_NUMBER = 1681511377;

        /** Deserialize all bitmaps from the `buffer` into a fresh array. */
        static RoaringBitmap[] deserialize(ByteBuffer buffer) throws IOException {
            long numberOfBitmaps = buffer.getLong();
            if (numberOfBitmaps < 0) {
                throw new IOException(String.format(
                        "Invalid RoaringBitmapArray length (%s < 0)", numberOfBitmaps));
            }
            if (numberOfBitmaps > Integer.MAX_VALUE) {
                throw new IOException(String.format(
                        "Invalid RoaringBitmapArray length (%s > %s)", numberOfBitmaps, Integer.MAX_VALUE));
            }
            // This format is designed for sparse bitmaps, so numberOfBitmaps is only a lower bound for the
            // actual size of the array.
            int minimumArraySize = (int) numberOfBitmaps;
            ArrayList<RoaringBitmap> bitmaps = new ArrayList(minimumArraySize);
            int lastIndex = 0;
            for (long _ = 0; _ < numberOfBitmaps; _ ++) {
                int key = buffer.getInt();
                if (key < 0L) {
                    throw new IOException(String.format(
                            "Invalid unsigned entry in RoaringBitmapArray (%s)", key));
                }
                assert key >= lastIndex: "Keys are required to be sorted in ascending order.";
                // Fill gaps in sparse data.
                while (lastIndex < key) {
                    bitmaps.add(new RoaringBitmap());
                    lastIndex += 1;
                }
                RoaringBitmap bitmap = new RoaringBitmap();
                bitmap.deserialize(buffer);
                bitmaps.add(bitmap);
                lastIndex += 1;
                // RoaringBitmap.deserialize doesn't move the buffer's pointer
                buffer.position(buffer.position() + bitmap.serializedSizeInBytes());
            }
            return bitmaps.toArray(new RoaringBitmap[0]);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Methods implemented for testing only
    ////////////////////////////////////////////////////////////////////////////////

    static Tuple2<Integer, Integer> decomposeHighLowBytes(long value) {
        return new Tuple2(highBytes(value), lowBytes(value));
    }

    public void add(long value) {
        checkArgument(value >= 0 && value <= MAX_REPRESENTABLE_VALUE);
        Tuple2<Integer, Integer> tup = decomposeHighLowBytes(value); // (high, low)
        if (tup._1 >= bitmaps.length) {
            extendBitmaps(tup._1 + 1);
        }
        RoaringBitmap highBitmap = bitmaps[tup._1];
        highBitmap.add(tup._2);
    }

    private void extendBitmaps(int  newLength) {
        if (bitmaps.length == 0 && newLength == 1) {
            bitmaps = new RoaringBitmap[]{new RoaringBitmap()};
            return;
        }
        RoaringBitmap[] newBitmaps = new RoaringBitmap[newLength];
        System.arraycopy(
                bitmaps, // source
                0, // source start pos
                newBitmaps, // dest
                0, // dest start pos
                bitmaps.length); // number of entries to copy
        for (int i = 0; i < bitmaps.length; i++) {
            newBitmaps[i] = new RoaringBitmap();
        }
        bitmaps = newBitmaps;
    }

    public static RoaringBitmapArray create(long... values) {
        RoaringBitmapArray bitmap = new RoaringBitmapArray();
        for (long value : values) {
            bitmap.add(value);
        }
        return bitmap;
    }
}
