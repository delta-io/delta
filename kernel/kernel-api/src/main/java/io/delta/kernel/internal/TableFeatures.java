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

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.util.ColumnMapping;

/**
 * Contains utility methods related to the Delta table feature support in protocol.
 */
public class TableFeatures {

    ////////////////////
    // Helper Methods //
    ////////////////////

    public static void validateReadSupportedTable(Protocol protocol, Metadata metadata) {
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
                        case "typeWidening-dev": // fall through
                        case "timestampNtz": // fall through
                        case "vacuumProtocolCheck":
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported table feature: " + readerFeature);
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported reader protocol version: " + protocol.getMinReaderVersion());
        }
    }
}
