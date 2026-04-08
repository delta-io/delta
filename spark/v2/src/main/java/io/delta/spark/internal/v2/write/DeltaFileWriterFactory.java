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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
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
 * Custom {@link DataWriterFactory} that creates {@link
 * DeltaFileFormatWriter.PartitionedTaskAttemptContextImpl} for partitioned table writes.
 *
 * <p>{@link DelayedCommitProtocol#parsePartitions} requires {@code
 * PartitionedTaskAttemptContextImpl} to extract partition column types for proper partition value
 * encoding (e.g. UTC-normalized timestamps). Spark's standard {@code FileWriterFactory} creates a
 * plain {@code TaskAttemptContextImpl} which would cause a {@code ClassCastException}.
 *
 * <p>For unpartitioned tables, this factory behaves identically to {@code FileWriterFactory}.
 */
public class DeltaFileWriterFactory implements DataWriterFactory {

  private final WriteJobDescription description;
  private final DelayedCommitProtocol committer;

  private final String jobTrackerID;

  @SuppressWarnings("unchecked")
  DeltaFileWriterFactory(WriteJobDescription description, DelayedCommitProtocol committer) {
    this.description = description;
    this.committer = committer;
    this.jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date());
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    TaskAttemptContextImpl taskAttemptContext =
        createTaskAttemptContext(partitionId, (int) (taskId & Integer.MAX_VALUE));
    committer.setupTask(taskAttemptContext);

    @SuppressWarnings({"unchecked", "rawtypes"})
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
  private TaskAttemptContextImpl createTaskAttemptContext(int partitionId, int realTaskId) {
    org.apache.hadoop.mapreduce.JobID jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, 0);
    TaskID taskId = new TaskID(jobId, TaskType.MAP, partitionId);
    TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, realTaskId);

    Configuration hadoopConf = description.serializableHadoopConf().value();
    hadoopConf.set("mapreduce.job.id", jobId.toString());
    hadoopConf.set("mapreduce.task.id", taskId.toString());
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString());
    hadoopConf.setBoolean("mapreduce.task.ismap", true);
    hadoopConf.setInt("mapreduce.task.partition", 0);

    if (description.partitionColumns().isEmpty()) {
      return new TaskAttemptContextImpl(hadoopConf, taskAttemptId);
    }

    // Build Map[String, DataType] for PartitionedTaskAttemptContextImpl
    Map<String, DataType> partColToDataType = new LinkedHashMap<>();
    Iterator<Attribute> it = (Iterator<Attribute>) description.partitionColumns().iterator();
    while (it.hasNext()) {
      Attribute attr = it.next();
      partColToDataType.put(attr.name(), attr.dataType());
    }

    // Build Scala Map[String, DataType] for PartitionedTaskAttemptContextImpl
    @SuppressWarnings({"rawtypes", "unchecked"})
    Builder<Tuple2<String, DataType>, scala.collection.immutable.Map<String, DataType>> b =
        (Builder) Map$.MODULE$.newBuilder();
    for (Map.Entry<String, DataType> e : partColToDataType.entrySet()) {
      b.$plus$eq(new Tuple2<>(e.getKey(), e.getValue()));
    }

    return new DeltaFileFormatWriter.PartitionedTaskAttemptContextImpl(
        hadoopConf, taskAttemptId, b.result());
  }
}
