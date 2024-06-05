/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.internal;

import java.util.List;

import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.ColumnMapping;
import static io.delta.kernel.internal.DeltaErrors.*;

/**
 * Contains utility methods related to the Delta table feature support in protocol.
 */
public class TableFeatures {

    ////////////////////
    // Helper Methods //
    ////////////////////

    public static void validateReadSupportedTable(
            Protocol protocol, Metadata metadata, String tablePath) {
        switch (protocol.getMinReaderVersion()) {
            case 1:
                break;
            case 2:
                ColumnMapping.throwOnUnsupportedColumnMappingMode(metadata);
                break;
            case 3:
                List<String> readerFeatures = protocol.getReaderFeatures();
                for (String readerFeature : readerFeatures) {
                    switch (readerFeature) {
                        case "columnMapping":
                            ColumnMapping.throwOnUnsupportedColumnMappingMode(metadata);
                            break;
                        case "deletionVectors": // fall through
                        case "timestampNtz": // fall through
                        case "vacuumProtocolCheck": // fall through
                        case "v2Checkpoint":
                            break;
                        default:
                            throw DeltaErrors.unsupportedReaderFeature(tablePath, readerFeature);
                    }
                }
                break;
            default:
                throw DeltaErrors.unsupportedReaderProtocol(
                    tablePath, protocol.getMinReaderVersion());
        }
    }

    /**
     * Utility method to validate whether the given table is supported for writing from Kernel.
     * Currently, the support is as follows:
     * <ul>
     *     <li>protocol writer version 1.</li>
     *     <li>protocol writer version 2 only with appendOnly feature enabled.</li>
     *     <li>protocol writer version 7 with {@code appendOnly}, {@code inCommitTimestamp-preview}
     *     feature enabled.</li>
     * </ul>
     *
     * @param protocol    Table protocol
     * @param metadata    Table metadata
     * @param tableSchema Table schema
     */
    public static void validateWriteSupportedTable(
            Protocol protocol,
            Metadata metadata,
            StructType tableSchema,
            String tablePath) {
        int minWriterVersion = protocol.getMinWriterVersion();
        switch (minWriterVersion) {
            case 1:
                break;
            case 2:
                // Append-only and column invariants are the writer features added in version 2
                // Append-only is supported, but not the invariants
                validateNoInvariants(tableSchema);
                break;
            case 3:
                // Check constraints are added in version 3
                throw unsupportedWriterProtocol(tablePath, minWriterVersion);
            case 4:
                // CDF and generated columns are writer features added in version 4
                throw unsupportedWriterProtocol(tablePath, minWriterVersion);
            case 5:
                // Column mapping is the only one writer feature added in version 5
                throw unsupportedWriterProtocol(tablePath, minWriterVersion);
            case 6:
                // Identity is the only one writer feature added in version 6
                throw unsupportedWriterProtocol(tablePath, minWriterVersion);
            case 7:
                for (String writerFeature : protocol.getWriterFeatures()) {
                    switch (writerFeature) {
                        // Only supported writer features as of today in Kernel
                        case "appendOnly":
                            break;
                        case "inCommitTimestamp-preview":
                            break;
                        default:
                            throw unsupportedWriterFeature(tablePath, writerFeature);
                    }
                }
                break;
            default:
                throw unsupportedWriterProtocol(tablePath, minWriterVersion);
        }
    }

    private static void validateNoInvariants(StructType tableSchema) {
        boolean hasInvariants = tableSchema.fields().stream().anyMatch(
                field -> field.getMetadata().contains("delta.invariants"));
        if (hasInvariants) {
            throw columnInvariantsNotSupported();
        }
    }
}
