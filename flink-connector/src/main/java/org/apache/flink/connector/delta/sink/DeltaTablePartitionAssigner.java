/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.delta.sink;

import java.io.Serializable;
import java.util.LinkedHashMap;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.utils.PartitionPathUtils;

/**
 * Custom implementation of {@link BucketAssigner} class required to provide behaviour on how
 * to map particular events to buckets (aka partitions).
 * <p>
 * This implementation can be perceived as a utility class for complying to the DeltaLake's
 * partitioning style (that follows Apache Hive's partitioning style by providing the partitioning
 * column's and its values as FS directories paths, e.g. "/some_path/table_1/date=2020-01-01")
 * It's still possible for users to roll out their own version of {@link BucketAssigner}
 * and pass it to the {@link DeltaSinkBuilder} during creation of the sink.
 *
 * @param <T> The type of input elements.
 */
public class DeltaTablePartitionAssigner<T> implements BucketAssigner<T, String> {

    private static final long serialVersionUID = -6033643154550226022L;

    private final DeltaPartitionComputer<T> partitionComputer;

    public DeltaTablePartitionAssigner(DeltaPartitionComputer<T> partitionComputer) {
        this.partitionComputer = partitionComputer;
    }

    @Override
    public String getBucketId(T element, Context context) {
        LinkedHashMap<String, String> partitionValues =
            this.partitionComputer.generatePartitionValues(element, context);
        return PartitionPathUtils.generatePartitionPath(partitionValues);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return "DeltaTablePartitionAssigner";
    }

    public interface DeltaPartitionComputer<T> extends Serializable {

        /**
         * Compute partition values from record.
         * <p>
         * E.g.
         * If the table has two partitioning columns 'date' and 'country' then this method should
         * return linked hashmap like:
         * LinkedHashMap(
         * "date" -&gt; "2020-01-01",
         * "country" -&gt; "x"
         * )
         * <p>
         * for event that should be written to example path of:
         * '/some_path/table_1/date=2020-01-01/country=x'.
         *
         * @param element input record.
         * @param context {@link Context} that can be used during partition's
         *                assignment
         * @return partition values.
         */
        LinkedHashMap<String, String> generatePartitionValues(
            T element, Context context);
    }
}
