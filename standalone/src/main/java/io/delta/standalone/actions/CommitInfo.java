/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Holds provenance information about changes to the table. This CommitInfo
 * is not stored in the checkpoint and has reduced compatibility guarantees.
 * Information stored in it is best effort (i.e. can be falsified by a writer).
 *
 * @see  <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md">Delta Transaction Log Protocol</a>
 */
public class CommitInfo implements Action {
    @Nonnull private final Optional<Long> version;
    @Nullable private final Timestamp timestamp;
    @Nonnull private final Optional<String> userId;
    @Nonnull private final Optional<String> userName;
    @Nullable private final String operation;
    @Nullable private final Map<String, String> operationParameters;
    @Nonnull private final Optional<JobInfo> jobInfo;
    @Nonnull private final Optional<NotebookInfo> notebookInfo;
    @Nonnull private final Optional<String> clusterId;
    @Nonnull private final Optional<Long> readVersion;
    @Nonnull private final Optional<String> isolationLevel;
    @Nonnull private final Optional<Boolean> isBlindAppend;
    @Nonnull private final Optional<Map<String, String>> operationMetrics;
    @Nonnull private final Optional<String> userMetadata;
    @Nonnull private final Optional<String> engineInfo;

    // For binary compatibility with version 0.2.0
    public CommitInfo(
            @Nonnull Optional<Long> version,
            @Nullable Timestamp timestamp,
            @Nonnull Optional<String> userId,
            @Nonnull Optional<String> userName,
            @Nullable String operation,
            @Nullable Map<String, String> operationParameters,
            @Nonnull Optional<JobInfo> jobInfo,
            @Nonnull Optional<NotebookInfo> notebookInfo,
            @Nonnull Optional<String> clusterId,
            @Nonnull Optional<Long> readVersion,
            @Nonnull Optional<String> isolationLevel,
            @Nonnull Optional<Boolean> isBlindAppend,
            @Nonnull Optional<Map<String, String>> operationMetrics,
            @Nonnull Optional<String> userMetadata) {
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
        this.engineInfo = Optional.empty();
    }

    public CommitInfo(
            @Nonnull Optional<Long> version,
            @Nullable Timestamp timestamp,
            @Nonnull Optional<String> userId,
            @Nonnull Optional<String> userName,
            @Nullable String operation,
            @Nullable Map<String, String> operationParameters,
            @Nonnull Optional<JobInfo> jobInfo,
            @Nonnull Optional<NotebookInfo> notebookInfo,
            @Nonnull Optional<String> clusterId,
            @Nonnull Optional<Long> readVersion,
            @Nonnull Optional<String> isolationLevel,
            @Nonnull Optional<Boolean> isBlindAppend,
            @Nonnull Optional<Map<String, String>> operationMetrics,
            @Nonnull Optional<String> userMetadata,
            @Nonnull Optional<String> engineInfo) {
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
        this.engineInfo = engineInfo;
    }

    /**
     * @return the log version for this commit
     */
    @Nonnull
    public Optional<Long> getVersion() {
        return version;
    }

    /**
     * @return the time the files in this commit were committed
     */
    @Nullable
    public Timestamp getTimestamp() {
        return timestamp;
    }

    /**
     * @return the userId of the user who committed this file
     */
    @Nonnull
    public Optional<String> getUserId() {
        return userId;
    }

    /**
     * @return the userName of the user who committed this file
     */
    @Nonnull
    public Optional<String> getUserName() {
        return userName;
    }

    /**
     * @return the type of operation for this commit. e.g. "WRITE"
     */
    @Nullable
    public String getOperation() {
        return operation;
    }

    /**
     * @return any relevant operation parameters. e.g. "mode", "partitionBy"
     */
    @Nullable
    public Map<String, String> getOperationParameters() {
        if (operationParameters != null) return Collections.unmodifiableMap(operationParameters);
        return null;
    }

    /**
     * @return the JobInfo for this commit
     */
    @Nonnull
    public Optional<JobInfo> getJobInfo() {
        return jobInfo;
    }

    /**
     * @return the NotebookInfo for this commit
     */
    @Nonnull
    public Optional<NotebookInfo> getNotebookInfo() {
        return notebookInfo;
    }

    /**
     * @return the ID of the cluster used to generate this commit
     */
    @Nonnull
    public Optional<String> getClusterId() {
        return clusterId;
    }

    /**
     * @return the version that the transaction used to generate this commit is reading from
     */
    @Nonnull
    public Optional<Long> getReadVersion() {
        return readVersion;
    }

    /**
     * @return the isolation level at which this commit was generated
     */
    @Nonnull
    public Optional<String> getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * @return whether this commit has blindly appended without caring about existing files
     */
    @Nonnull
    public Optional<Boolean> getIsBlindAppend() {
        return isBlindAppend;
    }

    /**
     * @return any operation metrics calculated
     */
    @Nonnull
    public Optional<Map<String, String>> getOperationMetrics() {
        return operationMetrics.map(Collections::unmodifiableMap);
    }

    /**
     * @return any additional user metadata
     */
    @Nonnull
    public Optional<String> getUserMetadata() {
        return userMetadata;
    }

    /**
     * @return the engineInfo of the operation that performed this commit. It should be of the form
     *         "{engineName}/{engineVersion} Delta-Standalone/{deltaStandaloneVersion}"
     */
    @Nonnull
    public Optional<String> getEngineInfo() {
        return engineInfo;
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
                Objects.equals(userMetadata, that.userMetadata) &&
                Objects.equals(engineInfo, that.engineInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, timestamp, userId, userName, operation, operationParameters,
                jobInfo, notebookInfo, clusterId, readVersion, isolationLevel, isBlindAppend,
                operationMetrics, userMetadata, engineInfo);
    }

    /**
     * @return a new {@code CommitInfo.Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for CommitInfo. Enables construction of CommitInfo object with default values.
     */
    public static class Builder {
        @Nonnull private Optional<Long> version = Optional.empty();
        @Nullable private Timestamp timestamp;
        @Nonnull private Optional<String> userId = Optional.empty();
        @Nonnull private Optional<String> userName = Optional.empty();
        @Nullable private String operation;
        @Nullable private Map<String, String> operationParameters;
        @Nonnull private Optional<JobInfo> jobInfo = Optional.empty();
        @Nonnull private Optional<NotebookInfo> notebookInfo = Optional.empty();
        @Nonnull private Optional<String> clusterId = Optional.empty();
        @Nonnull private Optional<Long> readVersion = Optional.empty();
        @Nonnull private Optional<String> isolationLevel = Optional.empty();
        @Nonnull private Optional<Boolean> isBlindAppend = Optional.empty();
        @Nonnull private Optional<Map<String, String>> operationMetrics = Optional.empty();
        @Nonnull private Optional<String> userMetadata = Optional.empty();
        @Nonnull private Optional<String> engineInfo = Optional.empty();

        public Builder version(Long version) {
            this.version = Optional.of(version);
            return this;
        }

        public Builder timestamp(@Nullable Timestamp timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder userId(@Nullable String userId) {
            this.userId = Optional.of(userId);
            return this;
        }

        public Builder userName(String userName) {
            this.userName = Optional.of(userName);
            return this;
        }

        public Builder operation(String operation) {
            this.operation = operation;
            return this;
        }

        public Builder operationParameters(@Nullable Map<String, String> operationParameters) {
            this.operationParameters = operationParameters;
            return this;
        }

        public Builder jobInfo(JobInfo jobInfo) {
            this.jobInfo = Optional.of(jobInfo);
            return this;
        }

        public Builder notebookInfo(NotebookInfo notebookInfo ) {
            this.notebookInfo = Optional.of(notebookInfo);
            return this;
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = Optional.of(clusterId);
            return this;
        }

        public Builder readVersion(Long readVersion) {
            this.readVersion = Optional.of(readVersion);
            return this;
        }

        public Builder isolationLevel(String isolationLevel) {
            this.isolationLevel = Optional.of(isolationLevel);
            return this;
        }

        public Builder isBlindAppend(Boolean isBlindAppend) {
            this.isBlindAppend = Optional.of(isBlindAppend);
            return this;
        }

        public Builder operationMetrics(Map<String, String> operationMetrics) {
            this.operationMetrics = Optional.of(operationMetrics);
            return this;
        }

        public Builder userMetadata(String userMetadata) {
            this.userMetadata = Optional.of(userMetadata);
            return this;
        }

        public Builder engineInfo(String engineInfo) {
            this.engineInfo = Optional.of(engineInfo);
            return this;
        }

        /**
         * @return a new {@code CommitInfo} with the same properties as {@code this}
         */
        public CommitInfo build() {
            CommitInfo commitInfo = new CommitInfo(this.version, this.timestamp, this.userId,
                    this.userName, this.operation, this.operationParameters, this.jobInfo,
                    this.notebookInfo, this.clusterId, this.readVersion, this.isolationLevel,
                    this.isBlindAppend, this.operationMetrics, this.userMetadata, this.engineInfo);
            return commitInfo;
        }
    }
}
