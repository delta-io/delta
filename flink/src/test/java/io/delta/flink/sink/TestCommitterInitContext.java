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

import java.util.OptionalLong;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

public class TestCommitterInitContext implements CommitterInitContext {
  private final int subtaskId;
  private final int parallelism;
  private final int attempt;
  private final JobInfo jobInfo;
  private final TaskInfo taskInfo;
  private final SinkCommitterMetricGroup metricGroup =
      InternalSinkCommitterMetricGroup.wrap(
          UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup());

  public TestCommitterInitContext(int subtaskId, int parallelism, int attempt) {
    this.subtaskId = subtaskId;
    this.parallelism = parallelism;
    this.attempt = attempt;

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
            return TestCommitterInitContext.this.parallelism;
          }

          @Override
          public int getNumberOfParallelSubtasks() {
            return TestCommitterInitContext.this.parallelism;
          }

          @Override
          public int getIndexOfThisSubtask() {
            return TestCommitterInitContext.this.subtaskId;
          }

          @Override
          public int getAttemptNumber() {
            return TestCommitterInitContext.this.attempt;
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
  public SinkCommitterMetricGroup metricGroup() {
    return metricGroup;
  }

  @Override
  public OptionalLong getRestoredCheckpointId() {
    return OptionalLong.empty();
  }

  @Override
  public JobInfo getJobInfo() {
    return jobInfo;
  }

  @Override
  public TaskInfo getTaskInfo() {
    return taskInfo;
  }
}
