package io.delta.standalone.actions;

import java.util.Objects;

public class JobInfo {
    private final String jobId;
    private final String jobName;
    private final String runId;
    private final String jobOwnerId;
    private final String triggerType;

    public JobInfo(String jobId, String jobName, String runId, String jobOwnerId, String triggerType) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.runId = runId;
        this.jobOwnerId = jobOwnerId;
        this.triggerType = triggerType;
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getRunId() {
        return runId;
    }

    public String getJobOwnerId() {
        return jobOwnerId;
    }

    public String getTriggerType() {
        return triggerType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobInfo jobInfo = (JobInfo) o;
        return Objects.equals(jobId, jobInfo.jobId) &&
                Objects.equals(jobName, jobInfo.jobName) &&
                Objects.equals(runId, jobInfo.runId) &&
                Objects.equals(jobOwnerId, jobInfo.jobOwnerId) &&
                Objects.equals(triggerType, jobInfo.triggerType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobName, runId, jobOwnerId, triggerType);
    }
}
