/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

public class TestWriterInitContext implements WriterInitContext {

  private final int subtaskId;
  private final int parallelism;
  private final int attempt;
  private final TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
  private final MailboxExecutor mailboxExecutor;
  private final SinkWriterMetricGroup metricGroup;
  private final JobInfo jobInfo;
  private final TaskInfo taskInfo;

  public TestWriterInitContext(int subtaskId, int parallelism, int attempt) {
    this.subtaskId = subtaskId;
    this.parallelism = parallelism;
    this.attempt = attempt;

    this.mailboxExecutor =
        new MailboxExecutorImpl(
            new TaskMailboxImpl(Thread.currentThread()),
            Integer.MAX_VALUE,
            StreamTaskActionExecutor.IMMEDIATE);

    this.metricGroup =
        InternalSinkWriterMetricGroup.wrap(
            UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup());

    this.jobInfo =
        new JobInfo() {
          @Override
          public JobID getJobId() {
            return JobID.generate();
          }

          @Override
          public String getJobName() {
            return "test-job";
          }
        };

    this.taskInfo =
        new TaskInfo() {
          @Override
          public String getTaskName() {
            return "test-task";
          }

          @Override
          public int getMaxNumberOfParallelSubtasks() {
            return TestWriterInitContext.this.parallelism;
          }

          @Override
          public int getNumberOfParallelSubtasks() {
            return TestWriterInitContext.this.parallelism;
          }

          @Override
          public int getIndexOfThisSubtask() {
            return TestWriterInitContext.this.subtaskId;
          }

          @Override
          public int getAttemptNumber() {
            return TestWriterInitContext.this.attempt;
          }

          @Override
          public String getTaskNameWithSubtasks() {
            return "aaa";
          }

          @Override
          public String getAllocationIDAsString() {
            return "bbb";
          }
        };
  }

  @Override
  public int getSubtaskId() {
    return subtaskId;
  }

  @Override
  public int getNumberOfParallelSubtasks() {
    return parallelism;
  }

  @Override
  public int getAttemptNumber() {
    return attempt;
  }

  @Override
  public SinkWriterMetricGroup metricGroup() {
    return metricGroup;
  }

  @Override
  public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
    return null;
  }

  @Override
  public MailboxExecutor getMailboxExecutor() {
    return mailboxExecutor;
  }

  @Override
  public ProcessingTimeService getProcessingTimeService() {
    return new ProcessingTimeService() {
      @Override
      public long getCurrentProcessingTime() {
        return processingTimeService.getCurrentProcessingTime();
      }

      @Override
      public ScheduledFuture<?> registerTimer(long time, ProcessingTimeCallback callback) {
        return processingTimeService.registerTimer(time, callback::onProcessingTime);
      }
    };
  }

  @Override
  public UserCodeClassLoader getUserCodeClassLoader() {
    return SimpleUserCodeClassLoader.create(Thread.currentThread().getContextClassLoader());
  }

  @Override
  public OptionalLong getRestoredCheckpointId() {
    return OptionalLong.empty();
  }

  @Override
  public boolean isObjectReuseEnabled() {
    return false;
  }

  @Override
  public <IN> TypeSerializer<IN> createInputSerializer() {
    return null;
  }

  @Override
  public JobInfo getJobInfo() {
    return jobInfo;
  }

  @Override
  public TaskInfo getTaskInfo() {
    return taskInfo;
  }

  @Override
  public <MetaT> Optional<Consumer<MetaT>> metadataConsumer() {
    return Optional.empty();
  }
}
