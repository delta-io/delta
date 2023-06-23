package io.delta.kernel.internal.deletionvectors;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * This implements Base85 using the 4 byte block aligned encoding and character set from Z85.
 *
 * @see https://rfc.zeromq.org/spec/32/
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
        String chars = ".-:+=^!/*?&<>()[]{}@%$#";
        for (int i2 = 0; i2 < chars.length(); i2 ++) {
            char c = chars.charAt(i2);
            map[i] = (byte) c;
            i ++;
        }
        return map;
    }

    private static byte[] getDecodeMap() {
        if (ENCODE_MAP.length - 1 > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Requirement not satisfied: ENCODE_MAP.length - 1 <= Byte.MAX_VALUE");
        }
        // The bitmask is the same as largest possible value, so the length of the array must
        // be one greater.
        byte[] map = new byte[ASCII_BITMASK + 1];
        for (int i = 0; i < ASCII_BITMASK + 1; i++) {
            map[i] = -1;
        }
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
        if (input.length % 5 != 0) {
            throw new IllegalArgumentException("input should be 5 character aligned");
        }
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
        if ((inputCharDecoder.canary & ~ASCII_BITMASK) != 0) {
            throw new IllegalArgumentException("Input is not valid Z85: " + encoded);
        }
        buffer.rewind();
        return buffer;
    }

    private static UUID uuidFromByteBuffer(ByteBuffer buffer) {
        assert buffer.remaining() >= 16;
        long highBits = buffer.getLong();
        long lowBits = buffer.getLong();
        return new UUID(highBits, lowBits);
    }
}