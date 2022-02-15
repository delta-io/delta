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

package io.delta.flink.sink;

import java.io.Serializable;
import java.util.LinkedHashMap;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.data.RowData;
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
 * To create new instance of the assigner and set it for the Delta sink:
 * <pre>
 *     /////////////////////////////////////////////////////////////////////////////////
 *     // implements a custom partition computer
 *     /////////////////////////////////////////////////////////////////////////////////
 *     static class CustomPartitionColumnComputer implements
 *             DeltaTablePartitionAssigner.DeltaPartitionComputer&lt;RowData&gt; {
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
 *     DeltaTablePartitionAssigner&lt;RowData&gt; partitionAssigner =
 *                 new DeltaTablePartitionAssigner&lt;&gt;(new CustomPartitionColumnComputer());
 *
 *     ...
 *
 *     /////////////////////////////////////////////////////////////////////////////////
 *     // creates an instance of partition assigner that extracts partition values from
 *     // {@link RowData} events
 *     /////////////////////////////////////////////////////////////////////////////////
 *     RowType rowType = ...;*
 *     List&lt;String&gt; partitionCols = ...; // list of partition columns' names
 *
 *     DeltaTablePartitionAssigner.DeltaRowDataPartitionComputer rowDataPartitionComputer =
 *         new DeltaTablePartitionAssigner.DeltaRowDataPartitionComputer(rowType, partitionCols);
 *
 *     DeltaTablePartitionAssigner&lt;RowData&gt; partitionAssigner =
 *         new DeltaTablePartitionAssigner&lt;&gt;(rowDataPartitionComputer);
 *
 *     ...
 *
 *     /////////////////////////////////////////////////////////////////////////////////
 *     // shows how to set a partition assigner for {@link DeltaSink}
 *     /////////////////////////////////////////////////////////////////////////////////
 *
 *     DeltaSink&lt;RowData&gt; deltaSink = DeltaSink.forRowData(
 *             new Path(deltaTablePath),
 *             new Configuration(),
 *             rowType)
 *         .withBucketAssigner(partitionAssigner)
 *         .build();
 *     stream.sinkTo(deltaSink);
 * </pre>
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
