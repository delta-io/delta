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

package io.delta.flink.sink.internal;

import java.util.LinkedHashMap;

import io.delta.flink.sink.RowDataDeltaSinkBuilder;
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
 * <p>
 * This {@link DeltaBucketAssigner} is applicable only to {@link DeltaSinkBuilder} and not to
 * {@link RowDataDeltaSinkBuilder}. The former lets you use this
 * {@link DeltaBucketAssigner} to provide the required custom bucketing behaviour, while the latter
 * doesn't expose a custom bucketing API, and you can provide the partition column keys only.
 * <p>
 * Thus, this {@link DeltaBucketAssigner} is currently not exposed to the user through any public
 * API.
 * <p>
 * In the future, if you'd like to implement your own custom bucketing...
 * <pre>
 *     /////////////////////////////////////////////////////////////////////////////////
 *     // implements a custom partition computer
 *     /////////////////////////////////////////////////////////////////////////////////
 *     static class CustomPartitionColumnComputer implements DeltaPartitionComputer&lt;RowData&gt; {
 *
 *         &#64;Override
 *         public LinkedHashMap&lt;String, String&gt; generatePartitionValues(
 *                 RowData element, BucketAssigner.Context context) {
 *             String f1 = element.getString(0).toString();
 *             int f3 = element.getInt(2);
 *             LinkedHashMap&lt;String, String&gt; partitionSpec = new LinkedHashMap&lt;&gt;();
 *             partitionSpec.put("f1", f1);
 *             partitionSpec.put("f3", Integer.toString(f3));
 *             return partitionSpec;
 *         }
 *     }
 *     ...
 *     /////////////////////////////////////////
 *     // creates partition assigner for a custom partition computer
 *     /////////////////////////////////////////
 *     DeltaBucketAssignerInternal&lt;RowData&gt; partitionAssigner =
 *                 new DeltaBucketAssignerInternal&lt;&gt;(new CustomPartitionColumnComputer());
 *
 *     ...
 *
 *     /////////////////////////////////////////////////////////////////////////////////
 *     // create the builder
 *     /////////////////////////////////////////////////////////////////////////////////
 *
 *     DeltaSinkBuilder&lt;RowData&gt;&lt;/RowData&gt; foo =
 *      new DeltaSinkBuilder.DefaultDeltaFormatBuilder&lt;&gt;(
 *         ...,
 *         partitionAssigner,
 *         ...)
 * </pre>
 *
 * @param <T> The type of input elements.
 */
public class DeltaBucketAssigner<T> implements BucketAssigner<T, String> {

    private static final long serialVersionUID = -6033643154550226022L;

    private final DeltaPartitionComputer<T> partitionComputer;

    public DeltaBucketAssigner(DeltaPartitionComputer<T> partitionComputer) {
        this.partitionComputer = partitionComputer;
    }

    @Override
    public String getBucketId(T element, BucketAssigner.Context context) {
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
        return "DeltaBucketAssigner";
    }
}
