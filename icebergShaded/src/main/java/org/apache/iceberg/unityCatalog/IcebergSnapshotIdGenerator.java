/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.iceberg.unityCatalog;

import java.math.BigInteger;
import java.util.UUID;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

// Utils class for generating deterministic Iceberg snapshot ID based on Delta version and table ID
public class IcebergSnapshotIdGenerator {
    private static final BigInteger PRIME = new BigInteger("11400714819323198485");
    private static final BigInteger TWO_POW_64 = BigInteger.ONE.shiftLeft(64);
    private static final BigInteger LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);

    public static long encode(long version, UUID uuid) {
        Preconditions.checkArgument(version >= 0 && version < 1L << 44);
        // Get 20-bit UUID fragment
        long uuidFragment = uuid.getLeastSignificantBits() & 0xFFFFF;

        // Store 43-bit version + 20-bit UUID fragment
        BigInteger shiftedVersion = BigInteger.valueOf(version).shiftLeft(20);
        BigInteger combined = shiftedVersion.or(BigInteger.valueOf(uuidFragment));

        // Scrambling
        BigInteger scrambled = combined.multiply(PRIME).and(TWO_POW_64.subtract(BigInteger.ONE));

        // Final result
        return scrambled.and(LONG_MAX_VALUE).longValue();
    }

    public static long decode(long encoded) {
        Preconditions.checkArgument(encoded >= 0);
        // Modular inverse of PRIME mod 2^64
        BigInteger inversePrime = PRIME.modInverse(TWO_POW_64);

        // Undo scrambling
        BigInteger combined = BigInteger.valueOf(encoded)
                .multiply(inversePrime)
                .mod(TWO_POW_64)
                // Ensure it is positive.
                .and(LONG_MAX_VALUE);

        // Extract version
        return combined.shiftRight(20).longValue();
    }
}
