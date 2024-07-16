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
package io.delta.kernel.internal.actions;

import java.util.*;

import io.delta.kernel.data.*;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.TableFeatures;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.util.VectorUtils.stringArrayValue;

public class Protocol {

    public static Protocol fromColumnVector(ColumnVector vector, int rowId) {
        if (vector.isNullAt(rowId)) {
            return null;
        }

        return new Protocol(
            vector.getChild(0).getInt(rowId),
            vector.getChild(1).getInt(rowId),
            vector.getChild(2).isNullAt(rowId) ? Collections.emptyList() :
                VectorUtils.toJavaList(vector.getChild(2).getArray(rowId)),
            vector.getChild(3).isNullAt(rowId) ? Collections.emptyList() :
                VectorUtils.toJavaList(vector.getChild(3).getArray(rowId))
        );
    }

    public static final StructType FULL_SCHEMA = new StructType()
        .add("minReaderVersion", IntegerType.INTEGER, false /* nullable */)
        .add("minWriterVersion", IntegerType.INTEGER, false /* nullable */)
        .add("readerFeatures", new ArrayType(StringType.STRING, false /* contains null */))
        .add("writerFeatures", new ArrayType(StringType.STRING, false /* contains null */));

    private final int minReaderVersion;
    private final int minWriterVersion;
    private final List<String> readerFeatures;
    private final List<String> writerFeatures;
    private final Lazy<Set<String>> readerAndWriterFeatureNames;

    public static Protocol empty() {
        return new Protocol(3, 7);
    }

    private static boolean supportsReaderFeatures(int minReaderVersion) {
        return minReaderVersion >= 3;
    }

    private static boolean supportsWriterFeatures(int minWriterVersion) {
        return minWriterVersion >= 7;
    }

    public Protocol(int minReaderVersion, int minWriterVersion) {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
        this.readerFeatures =
                supportsReaderFeatures(minReaderVersion) ? Collections.emptyList() : null;
        this.writerFeatures =
                supportsWriterFeatures(minWriterVersion) ? Collections.emptyList() : null;
        this.readerAndWriterFeatureNames = getLazyReaderAndWriterFeatureNames();
    }

    public Protocol(
        int minReaderVersion,
        int minWriterVersion,
        List<String> readerFeatures,
        List<String> writerFeatures) {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
        this.readerFeatures = readerFeatures;
        this.writerFeatures = writerFeatures;
        this.readerAndWriterFeatureNames = getLazyReaderAndWriterFeatureNames();
    }

    public int getMinReaderVersion() {
        return minReaderVersion;
    }

    public int getMinWriterVersion() {
        return minWriterVersion;
    }

    public List<String> getReaderFeatures() {
        return readerFeatures;
    }

    public List<String> getWriterFeatures() {
        return writerFeatures;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Protocol{");
        sb.append("minReaderVersion=").append(minReaderVersion);
        sb.append(", minWriterVersion=").append(minWriterVersion);
        sb.append(", readerFeatures=").append(readerFeatures);
        sb.append(", writerFeatures=").append(writerFeatures);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Encode as a {@link Row} object with the schema {@link Protocol#FULL_SCHEMA}.
     *
     * @return {@link Row} object with the schema {@link Protocol#FULL_SCHEMA}
     */
    public Row toRow() {
        Map<Integer, Object> protocolMap = new HashMap<>();
        protocolMap.put(0, minReaderVersion);
        protocolMap.put(1, minWriterVersion);
        protocolMap.put(2, stringArrayValue(readerFeatures));
        protocolMap.put(3, stringArrayValue(writerFeatures));

        return new GenericRow(Protocol.FULL_SCHEMA, protocolMap);
    }

    public Protocol withNewWriterFeatures(
            Set<String> writerFeatures) {
        Tuple2<Integer, Integer> newProtocolVersions =
                TableFeatures.minProtocolVersionFromAutomaticallyEnabledFeatures(
                        writerFeatures);
        List<String> newWriterFeatures = new ArrayList<>(writerFeatures);
        if (this.writerFeatures != null) {
            newWriterFeatures.addAll(this.writerFeatures);
        }
        return new Protocol(
                newProtocolVersions._1,
                newProtocolVersions._2,
                this.readerFeatures == null ? null : new ArrayList<>(this.readerFeatures),
                newWriterFeatures);
    }

    /**
     * Check if a `feature` is supported by this protocol. This means the protocol supports
     * table features and references the feature.
     */
    public boolean isFeatureSupported(String feature) {
        return readerAndWriterFeatureNames.get().contains(feature);
    }

    private Lazy<Set<String>> getLazyReaderAndWriterFeatureNames() {
        return new Lazy<>(() -> {
            Set<String> names = new HashSet<>();
            if (readerFeatures != null) {
                names.addAll(readerFeatures);
            }
            if (writerFeatures != null) {
                names.addAll(writerFeatures);
            }
            return names;
        });
    }
}
