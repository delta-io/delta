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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static io.delta.kernel.internal.util.InternalUtils.checkArgument;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * This implements Base85 using the 4 byte block aligned encoding and character set from Z85.
 *
 * @see <a href="https://rfc.zeromq.org/spec/32/">Z85 encoding</a>
 *
 * Taken from https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/util/Codec.scala
 */
public final class Base85Codec {

    static final long BASE = 85L;
    static final long BASE_2ND_POWER = 7225L; // 85^2
    static final long BASE_3RD_POWER = 614125L; // 85^3
    static final long BASE_4TH_POWER = 52200625L; // 85^4
    static final int ASCII_BITMASK = 0x7F;

    // UUIDs always encode into 20 characters.
    public static final int ENCODED_UUID_LENGTH = 20;

    private static byte[] getEncodeMap() {
        byte[] map = new byte[85];
        int i = 0;
        for (char c = '0'; c <= '9'; c++) {
            map[i] = (byte) c;
            i ++;
        }
        for (char c = 'a'; c <= 'z'; c ++) {
            map[i] = (byte) c;
            i ++;
        }
        for (char c = 'A'; c <= 'Z'; c ++) {
            map[i] = (byte) c;
            i ++;
        }
        for (char c: ".-:+=^!/*?&<>()[]{}@%$#".toCharArray()) {
            map[i] = (byte) c;
            i ++;
        }
        return map;
    }

    private static byte[] getDecodeMap() {
        checkArgument(ENCODE_MAP.length - 1 <= Byte.MAX_VALUE);
        // The bitmask is the same as largest possible value, so the length of the array must
        // be one greater.
        byte[] map = new byte[ASCII_BITMASK + 1];
        Arrays.fill(map, (byte) -1);
        for (int i = 0; i < ENCODE_MAP.length; i ++) {
            byte b = ENCODE_MAP[i];
            map[b] = (byte) i;
        }
        return map;
    }

    public static final byte[] ENCODE_MAP = getEncodeMap();
    public static final byte[] DECODE_MAP = getDecodeMap();


    /**
     * Decode a 16 byte UUID.
     */
    public static UUID decodeUUID(String encoded) {
        ByteBuffer buffer = decodeBlocks(encoded);
        return uuidFromByteBuffer(buffer);
    }

    /**
     * Decode an arbitrary byte array.
     *
     * Only `outputLength` bytes will be returned.
     * Any extra bytes, such as padding added because the input was unaligned, will be dropped.
     */
    public static byte[] decodeBytes(String encoded, int outputLength) {
        ByteBuffer result = decodeBlocks(encoded);
        if (result.remaining() > outputLength) {
            // Only read the expected number of bytes
            byte[] output = new byte[outputLength];
            result.get(output);
            return output;
        } else {
            return result.array();
        }
    }

    /**
     * Decode an arbitrary byte array.
     *
     * Output may contain padding bytes, if the input was not 4 byte aligned.
     * Use [[decodeBytes]] in that case and specify the expected number of output bytes
     * without padding.
     */
    public static byte[] decodeAlignedBytes(String encoded) {
        return decodeBlocks(encoded).array();
    }

    /**
     * Decode an arbitrary byte array.
     *
     * Output may contain padding bytes, if the input was not 4 byte aligned.
     */
    private static ByteBuffer decodeBlocks(String encoded) {
        char[] input = encoded.toCharArray();
        checkArgument(input.length % 5 == 0, "input should be 5 character aligned");
        ByteBuffer buffer = ByteBuffer.allocate(input.length / 5 * 4);

        // A mechanism to detect invalid characters in the input while decoding, that only has a
        // single conditional at the very end, instead of branching for every character.
        class InputCharDecoder {
            int canary = 0;

            long decodeInputChar(int i) {
                char c = input[i];
                canary |= c; // non-ascii char has bits outside of ASCII_BITMASK
                byte b = DECODE_MAP[c & ASCII_BITMASK];
                canary |= b; // invalid char maps to -1, which has bits outside ASCII_BITMASK
                return (long) b;
            }
        }

        int inputIndex = 0;
        InputCharDecoder inputCharDecoder = new InputCharDecoder();
        while (buffer.hasRemaining()) {
            long sum = 0;
            sum += inputCharDecoder.decodeInputChar(inputIndex) * BASE_4TH_POWER;
            sum += inputCharDecoder.decodeInputChar(inputIndex + 1) * BASE_3RD_POWER;
            sum += inputCharDecoder.decodeInputChar(inputIndex + 2) * BASE_2ND_POWER;
            sum += inputCharDecoder.decodeInputChar(inputIndex + 3) * BASE;
            sum += inputCharDecoder.decodeInputChar(inputIndex + 4);
            buffer.putInt((int) sum);
            inputIndex += 5;
        }
        checkArgument((inputCharDecoder.canary & ~ASCII_BITMASK)  == 0,
                "Input is not valid Z85: " + encoded);
        buffer.rewind();
        return buffer;
    }

    private static UUID uuidFromByteBuffer(ByteBuffer buffer) {
        checkArgument(buffer.remaining() >= 16);
        long highBits = buffer.getLong();
        long lowBits = buffer.getLong();
        return new UUID(highBits, lowBits);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Methods implemented for testing only
    ////////////////////////////////////////////////////////////////////////////////

    /** Encode a 16 byte UUID. */
    public static String encodeUUID(UUID id) {
        ByteBuffer buffer = uuidToByteBuffer(id);
        return encodeBlocks(buffer);
    }

    private static ByteBuffer uuidToByteBuffer(UUID id) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        buffer.rewind();
        return buffer;
    }

    /**
     * Encode an arbitrary byte array using 4 byte blocks.
     *
     * Expects the input to be 4 byte aligned.
     */
    private static String encodeBlocks(ByteBuffer buffer) {
        checkArgument(buffer.remaining() % 4 == 0);
        int numBlocks = buffer.remaining() / 4;
        // Every 4 byte block gets encoded into 5 bytes/chars
        int outputLength = numBlocks * 5;
        byte[] output = new byte[outputLength];
        int outputIndex = 0;

        while (buffer.hasRemaining()) {
            long sum = buffer.getInt() & 0x00000000ffffffffL;
            output[outputIndex] = ENCODE_MAP[(int) (sum / BASE_4TH_POWER)];
            sum %= BASE_4TH_POWER;
            output[outputIndex + 1] = ENCODE_MAP[(int) (sum / BASE_3RD_POWER)];
            sum %= BASE_3RD_POWER;
            output[outputIndex + 2] = ENCODE_MAP[(int) (sum / BASE_2ND_POWER)];
            sum %= BASE_2ND_POWER;
            output[outputIndex + 3] = ENCODE_MAP[(int) (sum / BASE)];
            output[outputIndex + 4] = ENCODE_MAP[(int) (sum % BASE)];
            outputIndex += 5;
        }

        return new String(output, US_ASCII);
    }

    /**
     * Encode an arbitrary byte array.
     *
     * Unaligned input will be padded to a multiple of 4 bytes.
     */
    public static String encodeBytes(byte[] input) {
        if (input.length % 4 == 0) {
            return encodeBlocks(ByteBuffer.wrap(input));
        } else {
            int alignedLength = ((input.length + 4) / 4) * 4;
            ByteBuffer buffer = ByteBuffer.allocate(alignedLength);
            buffer.put(input);
            while (buffer.hasRemaining()) {
                buffer.put((byte) 0);
            }
            buffer.rewind();
            return encodeBlocks(buffer);
        }
    }
}
