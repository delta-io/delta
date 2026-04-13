/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.write;

import java.util.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.internal.io.SparkHadoopWriterUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.delta.files.DelayedCommitProtocol;
import org.apache.spark.sql.delta.files.DeltaFileFormatWriter;
import org.apache.spark.sql.execution.datasources.DynamicPartitionDataSingleWriter;
import org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter;
import org.apache.spark.sql.execution.datasources.WriteJobDescription;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.DataType;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Map$;
import scala.collection.mutable.Builder;

/**
 * Custom {@link DataWriterFactory} for Delta DSv2 writes.
 *
 * <p>Delegates {@link TaskAttemptContext} creation to {@link
 * DeltaFileFormatWriter#createTaskAttemptContext}, which produces a {@link
 * DeltaFileFormatWriter.PartitionedTaskAttemptContextImpl} for partitioned tables. This is required
 * because {@link DelayedCommitProtocol#parsePartitions} needs partition column types for proper
 * partition value encoding (e.g. UTC-normalized timestamps).
 *
 * <p>Spark's standard {@code FileWriterFactory} always creates a plain {@code
 * TaskAttemptContextImpl}, which would cause a {@code ClassCastException} with {@link
 * DelayedCommitProtocol}.
 */
public class DeltaFileWriterFactory implements DataWriterFactory {

  private final WriteJobDescription description;
  private final DelayedCommitProtocol committer;

  private final String jobTrackerID;

  DeltaFileWriterFactory(WriteJobDescription description, DelayedCommitProtocol committer) {
    this.description = description;
    this.committer = committer;
    this.jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date());
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    scala.collection.immutable.Map<String, DataType> partColMap = buildPartitionColumnMap();

    TaskAttemptContext taskAttemptContext =
        DeltaFileFormatWriter.createTaskAttemptContext(
            description.serializableHadoopConf(),
            jobTrackerID,
            0,
            partitionId,
            (int) (taskId & Integer.MAX_VALUE),
            partColMap);
    committer.setupTask(taskAttemptContext);

    scala.collection.immutable.Map<String, SQLMetric> emptyMetrics =
        (scala.collection.immutable.Map) scala.collection.immutable.Map$.MODULE$.empty();

    if (description.partitionColumns().isEmpty()) {
      return new SingleDirectoryDataWriter(
          description, taskAttemptContext, committer, emptyMetrics);
    } else {
      return new DynamicPartitionDataSingleWriter(
          description, taskAttemptContext, committer, emptyMetrics);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private scala.collection.immutable.Map<String, DataType> buildPartitionColumnMap() {
    if (description.partitionColumns().isEmpty()) {
      return (scala.collection.immutable.Map) Map$.MODULE$.empty();
    }
    Builder<Tuple2<String, DataType>, scala.collection.immutable.Map<String, DataType>> b =
        (Builder) Map$.MODULE$.newBuilder();
    Iterator<Attribute> it = (Iterator<Attribute>) description.partitionColumns().iterator();
    while (it.hasNext()) {
      Attribute attr = it.next();
      b.$plus$eq(new Tuple2<>(attr.name(), attr.dataType()));
    }
    return b.result();
  }
}
