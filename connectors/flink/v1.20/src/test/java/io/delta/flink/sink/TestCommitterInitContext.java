package io.delta.flink.sink;

import java.util.OptionalLong;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

public class TestCommitterInitContext implements CommitterInitContext {
  private final int subtaskId;
  private final int parallelism;
  private final int attempt;
  private final JobInfo jobInfo;
  private final TaskInfo taskInfo;

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
    return null;
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
