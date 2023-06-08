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

import io.delta.kernel.data.Row;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;

public class Protocol implements Action {

    public static Protocol fromRow(Row row) {
        if (row == null) return null;
        final int minReaderVersion = row.getInt(0);
        final int minWriterVersion = row.getInt(1);
        return new Protocol(minReaderVersion, minWriterVersion);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("minReaderVersion", IntegerType.INSTANCE)
        .add("minWriterVersion", IntegerType.INSTANCE);

    private final int minReaderVersion;
    private final int minWriterVersion;

    public Protocol(int minReaderVersion, int minWriterVersion) {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
    }

    public int getMinReaderVersion()
    {
        return minReaderVersion;
    }

    public int getMinWriterVersion()
    {
        return minWriterVersion;
    }

    @Override
    public String toString() {
        return String.format("Protocol(%s,%s)", minReaderVersion, minWriterVersion);
    }
}
