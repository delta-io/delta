/*
 * Copyright (2020) The Delta Lake Project Authors.
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
package io.delta.standalone.actions;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Holds provenance information about changes to the table. This CommitInfo
 * is not stored in the checkpoint and has reduced compatibility guarantees.
 * Information stored in it is best effort (i.e. can be falsified by a writer).
 *
 * @see  <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md">Delta Transaction Log Protocol</a>
 */
public class CommitInfo {
    private final Optional<Long> version;
    private final Timestamp timestamp;
    private final Optional<String> userId;
    private final Optional<String> userName;
    private final String operation;
    private final Map<String, String> operationParameters;
    private final Optional<JobInfo> jobInfo;
    private final Optional<NotebookInfo> notebookInfo;
    private final Optional<String> clusterId;
    private final Optional<Long> readVersion;
    private final Optional<String> isolationLevel;
    private final Optional<Boolean> isBlindAppend;
    private final Optional<Map<String, String>> operationMetrics;
    private final Optional<String> userMetadata;

    public CommitInfo(Optional<Long> version, Timestamp timestamp, Optional<String> userId,
                      Optional<String> userName, String operation,
                      Map<String, String> operationParameters, Optional<JobInfo> jobInfo,
                      Optional<NotebookInfo> notebookInfo, Optional<String> clusterId,
                      Optional<Long> readVersion, Optional<String> isolationLevel,
                      Optional<Boolean> isBlindAppend,
                      Optional<Map<String, String>> operationMetrics,
                      Optional<String> userMetadata) {
        this.version = version;
        this.timestamp = timestamp;
        this.userId = userId;
        this.userName = userName;
        this.operation = operation;
        this.operationParameters = operationParameters;
        this.jobInfo = jobInfo;
        this.notebookInfo = notebookInfo;
        this.clusterId = clusterId;
        this.readVersion = readVersion;
        this.isolationLevel = isolationLevel;
        this.isBlindAppend = isBlindAppend;
        this.operationMetrics = operationMetrics;
        this.userMetadata = userMetadata;
    }

    /**
     * @return the log version for this commit
     */
    public Optional<Long> getVersion() {
        return version;
    }

    /**
     * @return the time the files in this commit were committed
     */
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return the userId of the user who committed this file
     */
    public Optional<String> getUserId() {
        return userId;
    }

    /**
     * @return the userName of the user who committed this file
     */
    public Optional<String> getUserName() {
        return userName;
    }

    /**
     * @return the type of operation for this commit. e.g. "WRITE"
     */
    public String getOperation() {
        return operation;
    }

    /**
     * @return any relevant operation parameters. e.g. "mode", "partitionBy"
     */
    public Map<String, String> getOperationParameters() {
        return Collections.unmodifiableMap(operationParameters);
    }

    /**
     * @return the JobInfo for this commit
     */
    public Optional<JobInfo> getJobInfo() {
        return jobInfo;
    }

    /**
     * @return the NotebookInfo for this commit
     */
    public Optional<NotebookInfo> getNotebookInfo() {
        return notebookInfo;
    }

    /**
     * @return the ID of the cluster used to generate this commit
     */
    public Optional<String> getClusterId() {
        return clusterId;
    }

    /**
     * @return the version that the transaction used to generate this commit is reading from
     */
    public Optional<Long> getReadVersion() {
        return readVersion;
    }

    /**
     * @return the isolation level at which this commit was generated
     */
    public Optional<String> getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * @return whether this commit has blindly appended without caring about existing files
     */
    public Optional<Boolean> getIsBlindAppend() {
        return isBlindAppend;
    }

    /**
     * @return any operation metrics calculated
     */
    public Optional<Map<String, String>> getOperationMetrics() {
        if (operationMetrics.isPresent()) {
            return Optional.of(Collections.unmodifiableMap(operationMetrics.get()));
        }
        return operationMetrics;
    }

    /**
     * @return any additional user metadata
     */
    public Optional<String> getUserMetadata() {
        return userMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommitInfo that = (CommitInfo) o;
        return Objects.equals(version, that.version) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(userName, that.userName) &&
                Objects.equals(operation, that.operation) &&
                Objects.equals(operationParameters, that.operationParameters) &&
                Objects.equals(jobInfo, that.jobInfo) &&
                Objects.equals(notebookInfo, that.notebookInfo) &&
                Objects.equals(clusterId, that.clusterId) &&
                Objects.equals(readVersion, that.readVersion) &&
                Objects.equals(isolationLevel, that.isolationLevel) &&
                Objects.equals(isBlindAppend, that.isBlindAppend) &&
                Objects.equals(operationMetrics, that.operationMetrics) &&
                Objects.equals(userMetadata, that.userMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, timestamp, userId, userName, operation, operationParameters,
                jobInfo, notebookInfo, clusterId, readVersion, isolationLevel, isBlindAppend,
                operationMetrics, userMetadata);
    }
}
