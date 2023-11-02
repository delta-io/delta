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

import java.util.Collections;
import java.util.List;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.util.VectorUtils;

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

    public static final StructType READ_SCHEMA = new StructType()
        .add("minReaderVersion", IntegerType.INTEGER, false /* nullable */)
        .add("minWriterVersion", IntegerType.INTEGER, false /* nullable */)
        .add("readerFeatures", new ArrayType(StringType.STRING, false /* contains null */))
        .add("writerFeatures", new ArrayType(StringType.STRING, false /* contains null */));

    private final int minReaderVersion;
    private final int minWriterVersion;
    private final List<String> readerFeatures;
    private final List<String> writerFeatures;

    public Protocol(
        int minReaderVersion,
        int minWriterVersion,
        List<String> readerFeatures,
        List<String> writerFeatures) {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
        this.readerFeatures = readerFeatures;
        this.writerFeatures = writerFeatures;
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
}
