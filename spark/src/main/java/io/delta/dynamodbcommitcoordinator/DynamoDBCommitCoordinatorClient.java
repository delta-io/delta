/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.dynamodbcommitcoordinator;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import io.delta.storage.CloseableIterator;
import io.delta.storage.LogStore;
import io.delta.storage.commit.*;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.io.*;
import java.util.*;

/**
 * A commit coordinator client that uses DynamoDB as the commit coordinator. The table schema is as follows:
 * tableId: String --- The unique identifier for the table. This is a UUID.
 * path: String --- The fully qualified path of the table in the file system. e.g. s3://bucket/path.
 * acceptingCommits: Boolean --- Whether the commit coordinator is accepting new commits. This will only
 *  be set to false when the table is converted from coordinated commits to file system commits.
 * tableVersion: Number --- The version of the latest commit.
 * tableTimestamp: Number --- The inCommitTimestamp of the latest commit.
 * schemaVersion: Number --- The version of the schema used to store the data.
 * hasAcceptedCommits: Boolean --- Whether any actual commits have been accepted by this commit coordinator
 *  after `registerTable`.
 * commits: --- The list of unbackfilled commits.
 *  version: Number --- The version of the commit.
 *  inCommitTimestamp: Number --- The inCommitTimestamp of the commit.
 *  fsName: String --- The name of the unbackfilled file.
 *  fsLength: Number --- The length of the unbackfilled file.
 *  fsTimestamp: Number --- The modification time of the unbackfilled file.
 */
public class DynamoDBCommitCoordinatorClient implements CommitCoordinatorClient {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBCommitCoordinatorClient.class);

    /**
     * The name of the DynamoDB table used to store unbackfilled commits.
     */
    final String coordinatedCommitsTableName;

    /**
     * The DynamoDB client used to interact with the DynamoDB table.
     */
    final DynamoDbClient client;

    /**
     * The endpoint of the DynamoDB table.
     */
    final String endpoint;

    /**
     * The number of write capacity units to provision for the DynamoDB table if the
     * client ends up creating a new one.
     */
    final long writeCapacityUnits;

    /**
     * The number of read capacity units to provision for the DynamoDB table if the
     * client ends up creating a new one.
     */
    final long readCapacityUnits;

    /**
     * The number of commits to batch backfill at once. A backfill is performed
     * whenever commitVersion % batchSize == 0.
     */
    public final long backfillBatchSize;

    /**
     * Whether we should skip matching the current table path against the one stored in DynamoDB
     * when interacting with it.
     */
    final boolean skipPathCheck;

    /**
     * The key used to store the tableId in the coordinated commits table configuration.
     */
    final static String TABLE_CONF_TABLE_ID_KEY = "tableId";

    /**
     * The version of the client. This is used to ensure that the client is compatible with the
     * schema of the data stored in the DynamoDB table. A client should only be able to
     * access a table if the schema version of the table matches the client version.
     */
    final int CLIENT_VERSION = 1;

    private static class GetCommitsResultInternal {
        final GetCommitsResponse response;
        final boolean hasAcceptedCommits;
        GetCommitsResultInternal(
                GetCommitsResponse response,
                boolean hasAcceptedCommits) {
            this.response = response;
            this.hasAcceptedCommits = hasAcceptedCommits;
        }
    }


    public DynamoDBCommitCoordinatorClient(
            String coordinatedCommitsTableName,
            String endpoint,
            DynamoDbClient client,
            long backfillBatchSize) throws IOException {
        this(
            coordinatedCommitsTableName,
            endpoint,
            client,
            backfillBatchSize,
            5 /* readCapacityUnits */,
            5 /* writeCapacityUnits */,
            false /* skipPathCheck */);
    }

    public DynamoDBCommitCoordinatorClient(
            String coordinatedCommitsTableName,
            String endpoint,
            DynamoDbClient client,
            long backfillBatchSize,
            long readCapacityUnits,
            long writeCapacityUnits,
            boolean skipPathCheck) throws IOException {
        this.coordinatedCommitsTableName = coordinatedCommitsTableName;
        this.endpoint = endpoint;
        this.client = client;
        this.backfillBatchSize = backfillBatchSize;
        this.readCapacityUnits = readCapacityUnits;
        this.writeCapacityUnits = writeCapacityUnits;
        this.skipPathCheck = skipPathCheck;
        tryEnsureTableExists();
    }

    private String getTableId(Map<String, String> coordinatedCommitsTableConf) {
        if (!coordinatedCommitsTableConf.containsKey(TABLE_CONF_TABLE_ID_KEY)) {
            throw new RuntimeException("tableId not found");
        }
        return coordinatedCommitsTableConf.get(TABLE_CONF_TABLE_ID_KEY);
    }

    /**
     * Fetches the entry from the commit coordinator for the given table. Only the attributes defined
     * in attributesToGet will be fetched.
     */
    private GetItemResponse getEntryFromCommitCoordinator(
            Map<String, String> coordinatedCommitsTableConf, String... attributesToGet) {
        GetItemRequest request = GetItemRequest.builder()
                .tableName(coordinatedCommitsTableName)
                .key(Collections.singletonMap(
                        DynamoDBTableEntryConstants.TABLE_ID,
                        AttributeValue.builder()
                                .s(getTableId(coordinatedCommitsTableConf))
                                .build()))
                .attributesToGet(attributesToGet)
                .build();
        return client.getItem(request);
    }

    /**
     * Commits the given file to the commit coordinator.
     * A conditional write is performed to the DynamoDB table entry associated with this Delta
     * table.
     * If the conditional write goes through, the filestatus of the UUID delta file will be
     * appended to the list of unbackfilled commits, and other updates like setting the latest
     * table version to `attemptVersion` will be performed.
     *
     * For the conditional write to go through, the following conditions must be met right before
     * the write is performed:
     * 1. The latest table version in DynamoDB is equal to attemptVersion - 1.
     * 2. The commit coordinator is accepting new commits.
     * 3. The schema version of the commit coordinator matches the schema version of the client.
     * 4. The table path stored in DynamoDB matches the path of the table. This check is skipped
     * if `skipPathCheck` is set to true.
     * If the conditional write fails, we retrieve the current entry in DynamoDB to figure out
     * which condition failed. (DynamoDB does not tell us which condition failed in the rejection.)
     * If any of (2), (3), or (4) fail, an unretryable `CommitFailedException` will be thrown.
     * For (1):
     * If the retrieved latest table version is greater than or equal to attemptVersion, a retryable
     * `CommitFailedException` will be thrown.
     * If the retrieved latest table version is less than attemptVersion - 1, an unretryable
     * `CommitFailedException` will be thrown.
     */
    protected CommitResponse commitToCoordinator(
            Path logPath,
            Map<String, String> coordinatedCommitsTableConf,
            long attemptVersion,
            FileStatus commitFile,
            long inCommitTimestamp,
            boolean isCCtoFSConversion) throws CommitFailedException {
        // Add conditions for the conditional update.
        java.util.Map<String, ExpectedAttributeValue> expectedValuesBeforeUpdate = new HashMap<>();
        expectedValuesBeforeUpdate.put(
                DynamoDBTableEntryConstants.TABLE_LATEST_VERSION,
                ExpectedAttributeValue.builder()
                        .value(AttributeValue.builder().n(Long.toString(attemptVersion - 1)).build())
                        .build()
        );
        expectedValuesBeforeUpdate.put(
                DynamoDBTableEntryConstants.ACCEPTING_COMMITS,
                ExpectedAttributeValue.builder()
                    .value(AttributeValue.builder().bool(true).build())
                    .build()
        );
        if (!skipPathCheck) {
            expectedValuesBeforeUpdate.put(
                    DynamoDBTableEntryConstants.TABLE_PATH,
                    ExpectedAttributeValue.builder()
                        .value(AttributeValue.builder().s(logPath.getParent().toString()).build())
                        .build()
            );
        }
        expectedValuesBeforeUpdate.put(
                DynamoDBTableEntryConstants.SCHEMA_VERSION,
                ExpectedAttributeValue.builder()
                    .value(AttributeValue.builder().n(Integer.toString(CLIENT_VERSION)).build())
                    .build()
        );

        java.util.Map<String, AttributeValue> newCommit = new HashMap<>();
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_VERSION,
                AttributeValue.builder().n(Long.toString(attemptVersion)).build());
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_TIMESTAMP,
                AttributeValue.builder().n(Long.toString(inCommitTimestamp)).build());
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_FILE_NAME,
                AttributeValue.builder().s(commitFile.getPath().getName()).build());
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_FILE_LENGTH,
                AttributeValue.builder().n(Long.toString(commitFile.getLen())).build());
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_FILE_MODIFICATION_TIMESTAMP,
                AttributeValue.builder().n(Long.toString(commitFile.getModificationTime())).build());

        UpdateItemRequest.Builder requestBuilder = UpdateItemRequest.builder()
                .tableName(coordinatedCommitsTableName)
                .key(Collections.singletonMap(
                        DynamoDBTableEntryConstants.TABLE_ID,
                        AttributeValue.builder()
                                .s(getTableId(coordinatedCommitsTableConf))
                                .build()))
                .attributeUpdates(new HashMap<String, AttributeValueUpdate>() {{
                    put(DynamoDBTableEntryConstants.TABLE_LATEST_VERSION,
                            AttributeValueUpdate.builder()
                                    .value(AttributeValue.builder()
                                            .n(Long.toString(attemptVersion))
                                            .build())
                                    .action(AttributeAction.PUT)
                                    .build());
                    // We need to set this to true to indicate that commits have been accepted after
                    // `registerTable`.
                    put(DynamoDBTableEntryConstants.HAS_ACCEPTED_COMMITS,
                            AttributeValueUpdate.builder()
                                    .value(AttributeValue.builder()
                                            .bool(true)
                                            .build())
                                    .action(AttributeAction.PUT)
                                    .build());
                    put(DynamoDBTableEntryConstants.TABLE_LATEST_TIMESTAMP,
                            AttributeValueUpdate.builder()
                                    .value(AttributeValue.builder()
                                            .n(Long.toString(inCommitTimestamp))
                                            .build())
                                    .action(AttributeAction.PUT)
                                    .build());
                    put(DynamoDBTableEntryConstants.COMMITS,
                            AttributeValueUpdate.builder()
                                    .action(AttributeAction.ADD)
                                    .value(AttributeValue.builder()
                                            .l(AttributeValue.builder()
                                                    .m(newCommit)
                                                    .build())
                                            .build())
                                    .build());
                }})
                .expected(expectedValuesBeforeUpdate);

        if (isCCtoFSConversion) {
            // If this table is being converted from coordinated commits to file system commits, we need
            // to set acceptingCommits to false.
            Map<String, AttributeValueUpdate> updates = new HashMap<>(requestBuilder.build().attributeUpdates());
            updates.put(DynamoDBTableEntryConstants.ACCEPTING_COMMITS,
                    AttributeValueUpdate.builder()
                            .value(AttributeValue.builder()
                                    .bool(false)
                                    .build())
                            .action(AttributeAction.PUT)
                            .build());
            requestBuilder.attributeUpdates(updates);
        }

        try {
            client.updateItem(requestBuilder.build());
        } catch (ConditionalCheckFailedException e) {
            // Conditional check failed. The exception will not indicate which condition failed.
            // We need to check the conditions ourselves by fetching the item and checking the
            // values.
            GetItemResponse latestEntry = getEntryFromCommitCoordinator(
                    coordinatedCommitsTableConf,
                    DynamoDBTableEntryConstants.TABLE_LATEST_VERSION,
                    DynamoDBTableEntryConstants.ACCEPTING_COMMITS,
                    DynamoDBTableEntryConstants.TABLE_PATH,
                    DynamoDBTableEntryConstants.SCHEMA_VERSION);

            int schemaVersion = Integer.parseInt(
                    latestEntry.item().get(DynamoDBTableEntryConstants.SCHEMA_VERSION).n());
            if (schemaVersion != CLIENT_VERSION) {
                throw new CommitFailedException(
                        false /* retryable */,
                        false /* conflict */,
                        "The schema version of the commit coordinator does not match the current" +
                                "DynamoDBCommitCoordinatorClient version. The data schema version is " +
                                " " + schemaVersion + " while the client version is " +
                                CLIENT_VERSION + ". Make sure that the correct client is being " +
                                "used to access this table." );
            }
            long latestTableVersion = Long.parseLong(
                    latestEntry.item().get(DynamoDBTableEntryConstants.TABLE_LATEST_VERSION).n());
            if (!skipPathCheck &&
                    !latestEntry.item().get("path").s().equals(logPath.getParent().toString())) {
                throw new CommitFailedException(
                        false /* retryable */,
                        false /* conflict */,
                        "This commit was attempted from path " + logPath.getParent() +
                                " while the table is registered at " +
                                latestEntry.item().get("path").s() + ".");
            }
            if (!latestEntry.item().get(DynamoDBTableEntryConstants.ACCEPTING_COMMITS).bool()) {
                throw new CommitFailedException(
                        false /* retryable */,
                        false /* conflict */,
                        "The commit coordinator is not accepting any new commits for this table.");
            }
            if (latestTableVersion != attemptVersion - 1) {
                // The commit is only retryable if the conflict is due to someone else committing
                // a version greater than the expected version.
                boolean retryable = latestTableVersion > attemptVersion - 1;
                throw new CommitFailedException(
                        retryable /* retryable */,
                        retryable /* conflict */,
                        "Commit version " + attemptVersion + " is not valid. Expected version: " +
                                (latestTableVersion + 1) + ".");
            }
        }
        Commit resultantCommit = new Commit(attemptVersion, commitFile, inCommitTimestamp);
        return new CommitResponse(resultantCommit);
    }

    @Override
    public CommitResponse commit(
            LogStore logStore,
            Configuration hadoopConf,
            TableDescriptor tableDesc,
            long commitVersion,
            Iterator<String> actions,
            UpdatedActions updatedActions) throws CommitFailedException {
        Path logPath = tableDesc.getLogPath();
        if (commitVersion == 0) {
            throw new CommitFailedException(
                    false /* retryable */,
                    false /* conflict */,
                    "Commit version 0 must go via filesystem.");
        }
        try {
            FileStatus commitFileStatus = CoordinatedCommitsUtils.writeUnbackfilledCommitFile(
                    logStore,
                    hadoopConf,
                    logPath.toString(),
                    commitVersion,
                    actions,
                    UUID.randomUUID().toString());
            long inCommitTimestamp = updatedActions.getCommitInfo().getCommitTimestamp();
            boolean isCCtoFSConversion =
                    CoordinatedCommitsUtils.isCoordinatedCommitsToFSConversion(commitVersion, updatedActions);

            LOG.info("Committing version {} with UUID delta file {} to DynamoDB.",
                    commitVersion, commitFileStatus.getPath());
            CommitResponse res = commitToCoordinator(
                    logPath,
                    tableDesc.getTableConf(),
                    commitVersion,
                    commitFileStatus,
                    inCommitTimestamp,
                    isCCtoFSConversion);

            LOG.info("Commit {} was successful.", commitVersion);

            boolean shouldBackfillOnEveryCommit = backfillBatchSize <= 1;
            boolean isBatchBackfillDue = commitVersion % backfillBatchSize == 0;
            boolean shouldBackfill = shouldBackfillOnEveryCommit || isBatchBackfillDue ||
                    // Always attempt a backfill for coordinated commits to filesystem conversion.
                    // Even if this fails, the next reader will attempt to backfill.
                    isCCtoFSConversion;
            if (shouldBackfill) {
                backfillToVersion(
                    logStore,
                    hadoopConf,
                    tableDesc,
                    commitVersion,
                    null /* lastKnownBackfilledVersion */);
            }
            return res;
        } catch (IOException e) {
            throw new CommitFailedException(false /* retryable */, false /* conflict */, e.getMessage(), e);
        }
    }

    private GetCommitsResultInternal getCommitsImpl(
            Path logPath,
            Map<String, String> tableConf,
            Long startVersion,
            Long endVersion) throws IOException {
        GetItemResponse latestEntry = getEntryFromCommitCoordinator(
                tableConf,
                DynamoDBTableEntryConstants.COMMITS,
                DynamoDBTableEntryConstants.TABLE_LATEST_VERSION,
                DynamoDBTableEntryConstants.HAS_ACCEPTED_COMMITS);

        java.util.Map<String, AttributeValue> item = latestEntry.item();
        long currentVersion =
                Long.parseLong(item.get(DynamoDBTableEntryConstants.TABLE_LATEST_VERSION).n());
        AttributeValue allStoredCommits = item.get(DynamoDBTableEntryConstants.COMMITS);
        ArrayList<Commit> commits = new ArrayList<>();
        Path unbackfilledCommitsPath = CoordinatedCommitsUtils.commitDirPath(logPath);
        for(AttributeValue attr: allStoredCommits.l()) {
            java.util.Map<String, AttributeValue> commitMap = attr.m();
            long commitVersion =
                    Long.parseLong(commitMap.get(DynamoDBTableEntryConstants.COMMIT_VERSION).n());
            boolean commitInRange = (startVersion == null || commitVersion >= startVersion) &&
                    (endVersion == null || endVersion >= commitVersion);
            if (commitInRange) {
                Path filePath = new Path(
                        unbackfilledCommitsPath,
                        commitMap.get(DynamoDBTableEntryConstants.COMMIT_FILE_NAME).s());
                long length =
                        Long.parseLong(commitMap.get(DynamoDBTableEntryConstants.COMMIT_FILE_LENGTH).n());
                long modificationTime = Long.parseLong(
                        commitMap.get(DynamoDBTableEntryConstants.COMMIT_FILE_MODIFICATION_TIMESTAMP).n());
                FileStatus fileStatus = new FileStatus(
                        length,
                        false /* isDir */,
                        0 /* blockReplication */,
                        0 /* blockSize */,
                        modificationTime,
                        filePath);
                long inCommitTimestamp =
                        Long.parseLong(commitMap.get(DynamoDBTableEntryConstants.COMMIT_TIMESTAMP).n());
                commits.add(new Commit(commitVersion, fileStatus, inCommitTimestamp));
            }
        }
        GetCommitsResponse response = new GetCommitsResponse(
                new ArrayList(commits), currentVersion);
        return new GetCommitsResultInternal(
                response,
                item.get(DynamoDBTableEntryConstants.HAS_ACCEPTED_COMMITS).bool());
    }

    @Override
    public GetCommitsResponse getCommits(
            TableDescriptor tableDesc,
            Long startVersion,
            Long endVersion) {
        try {
            GetCommitsResultInternal res =
                    getCommitsImpl(tableDesc.getLogPath(), tableDesc.getTableConf(), startVersion, endVersion);
            long latestTableVersionToReturn = res.response.getLatestTableVersion();
            if (!res.hasAcceptedCommits) {
                /*
                 * If the commit coordinator has not accepted any commits after `registerTable`, we should
                 * return -1 as the latest table version.
                 * ┌───────────────────────────────────┬─────────────────────────────────────────────────────┬────────────────────────────────┐
                 * │              Action               │                   Internal State                    │ Version returned on GetCommits │
                 * ├───────────────────────────────────┼─────────────────────────────────────────────────────┼────────────────────────────────┤
                 * │ Table is pre-registered at X      │ hasAcceptedCommits = false, latestTableVersion = X  │             -1                 │
                 * │ Commit X+1 after pre-registration │ hasAcceptedCommits = true, latestTableVersion = X+1 │             X+1                │
                 * └───────────────────────────────────┴─────────────────────────────────────────────────────┴────────────────────────────────┘
                */
                latestTableVersionToReturn = -1;
            }
            return new GetCommitsResponse(res.response.getCommits(), latestTableVersionToReturn);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes the given actions to a file.
     * logStore.write(overwrite=false) will throw a FileAlreadyExistsException if the file already
     * exists. However, the scala LogStore interface does not declare this as part of the function
     * signature. This method wraps the write method and declares the exception to ensure that the
     * caller is aware of the exception.
     */
    private void writeActionsToBackfilledFile(
            LogStore logStore,
            Path logPath,
            long version,
            Iterator<String> actions,
            Configuration hadoopConf,
            boolean shouldOverwrite) throws IOException {
        Path targetPath = CoordinatedCommitsUtils.getBackfilledDeltaFilePath(logPath, version);
        logStore.write(targetPath, actions, shouldOverwrite, hadoopConf);
    }

    private void validateBackfilledFileExists(
            Path logPath, Configuration hadoopConf, Long lastKnownBackfilledVersion) {
        try {
            if (lastKnownBackfilledVersion == null) {
                return;
            }
            Path lastKnownBackfilledFile = CoordinatedCommitsUtils.getBackfilledDeltaFilePath(
                logPath, lastKnownBackfilledVersion);
            FileSystem fs = logPath.getFileSystem(hadoopConf);
            if (!fs.exists(lastKnownBackfilledFile)) {
                throw new IllegalArgumentException(
                        "Expected backfilled file at " + lastKnownBackfilledFile + " does not exist.");
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Backfills all the unbackfilled commits returned by the commit coordinator and notifies the commit
     * owner of the backfills.
     * The version parameter is ignored in this implementation and all the unbackfilled commits
     * are backfilled. This method will not throw any exception if the physical backfill
     * succeeds but the update to the commit coordinator fails.
     * @throws IllegalArgumentException if the requested backfill version is greater than the latest
     *  version for the table.
     */
    @Override
    public void backfillToVersion(
            LogStore logStore,
            Configuration hadoopConf,
            TableDescriptor tableDesc,
            long version,
            Long lastKnownBackfilledVersion) throws IOException {
        LOG.info("Backfilling all unbackfilled commits.");
        Path logPath = tableDesc.getLogPath();
        GetCommitsResponse resp;
        try {
            resp = getCommitsImpl(
                    logPath,
                    tableDesc.getTableConf(),
                    lastKnownBackfilledVersion,
                    null).response;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        validateBackfilledFileExists(logPath, hadoopConf, lastKnownBackfilledVersion);
        if (version > resp.getLatestTableVersion()) {
            throw new IllegalArgumentException(
                    "The requested backfill version " + version + " is greater than the latest " +
                            "version " + resp.getLatestTableVersion() + " for the table.");
        }
        // If partial writes are visible in this filesystem, we should not try to overwrite existing
        // files. A failed overwrite can truncate the existing file.
        boolean shouldOverwrite = !logStore.isPartialWriteVisible(
                logPath,
                hadoopConf);
        for (Commit commit: resp.getCommits()) {
            CloseableIterator<String> actions =
                    logStore.read(commit.getFileStatus().getPath(), hadoopConf);
            try {
                writeActionsToBackfilledFile(
                        logStore,
                        logPath,
                        commit.getVersion(),
                        actions,
                        hadoopConf,
                        shouldOverwrite);
            } catch (java.nio.file.FileAlreadyExistsException e) {
                // Ignore the exception. This indicates that the file has already been backfilled.
                LOG.info("File {} already exists. Skipping backfill for this file.",
                        commit.getFileStatus().getPath());
            } finally {
                actions.close();
            }
        }

        Map<String, AttributeValue> requestKey = new HashMap<>();
        requestKey.put(DynamoDBTableEntryConstants.TABLE_ID,
                        AttributeValue.builder().s(getTableId(tableDesc.getTableConf())).build());


        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put(DynamoDBTableEntryConstants.COMMITS,
                AttributeValueUpdate.builder()
                        .action(AttributeAction.PUT)
                        .value(AttributeValue.builder().l(new ArrayList<>()).build())
                        .build());
        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(coordinatedCommitsTableName)
                .key(requestKey)
                .attributeUpdates(attributeUpdates)
                .expected(new HashMap<String, ExpectedAttributeValue>(){
                    {
                        put(DynamoDBTableEntryConstants.TABLE_LATEST_VERSION, ExpectedAttributeValue.builder()
                                .value(
                                        AttributeValue.builder()
                                                .n(Long.toString(resp.getLatestTableVersion())).build())
                                .build()
                        );
                        put(DynamoDBTableEntryConstants.TABLE_PATH, ExpectedAttributeValue.builder()
                                .value(
                                        AttributeValue.builder()
                                                .s(logPath.getParent().toString()).build())
                                .build()
                        );
                        put(DynamoDBTableEntryConstants.SCHEMA_VERSION, ExpectedAttributeValue.builder()
                                .value(
                                        AttributeValue.builder()
                                                .n(Integer.toString(CLIENT_VERSION)).build())
                                .build()
                        );
                    }
                }).build();
        try {
            client.updateItem(request);
        } catch (ConditionalCheckFailedException e) {
            // Ignore the exception. The backfill succeeded but the update to
            // the commit coordinator failed. The main purpose of a backfill operation is to ensure that
            // UUID commit is physically copied to a standard commit file path. A failed update to
            // the commit coordinator is not critical.
            LOG.warn("Backfill succeeded but the update to the commit coordinator failed. This is probably" +
                    " due to a concurrent update to the commit coordinator. This is not a critical error and " +
                    " should rectify itself.");
        }
    }

    @Override
    public Map<String, String> registerTable(
            Path logPath,
            Optional<TableIdentifier> tableIdentifier,
            long currentVersion,
            AbstractMetadata currentMetadata,
            AbstractProtocol currentProtocol) {
        java.util.Map<String, AttributeValue> item = new HashMap<>();

        String tableId = java.util.UUID.randomUUID().toString();
        item.put(DynamoDBTableEntryConstants.TABLE_ID, AttributeValue.builder().s(tableId).build());

        // We maintain the invariant that a commit will only succeed if the latestVersion stored
        // in the table is equal to attemptVersion - 1. To maintain this, even though the
        // filesystem-based commit after register table can fail, we still treat the attemptVersion
        // at registration as a valid version. Since it is expected that the commit coordinator will
        // return -1 as the table version if no commits have been accepted after registration, we
        // use another attribute (HAS_ACCEPTED_COMMITS) to track whether any commits have been
        // accepted. This attribute is set to true whenever any commit is accepted.
        // If HAS_ACCEPTED_COMMITS is false, in a getCommit request, we set the latest version to -1.
        long attemptVersion = currentVersion + 1;
        item.put(
                DynamoDBTableEntryConstants.TABLE_LATEST_VERSION,
                AttributeValue.builder().n(Long.toString(attemptVersion)).build());
        // Used to indicate that no real commits have gone through the commit coordinator yet.
        item.put(
                DynamoDBTableEntryConstants.HAS_ACCEPTED_COMMITS,
                AttributeValue.builder().bool(false).build());

        item.put(
                DynamoDBTableEntryConstants.TABLE_PATH,
                AttributeValue.builder().s(logPath.getParent().toString()).build());
        item.put(DynamoDBTableEntryConstants.COMMITS, AttributeValue.builder().l(new ArrayList<>()).build());
        item.put(
                DynamoDBTableEntryConstants.ACCEPTING_COMMITS, AttributeValue.builder().bool(true).build());
        item.put(
                DynamoDBTableEntryConstants.SCHEMA_VERSION,
                AttributeValue.builder().n(Integer.toString(CLIENT_VERSION)).build());

        PutItemRequest request = PutItemRequest.builder()
                .tableName(coordinatedCommitsTableName)
                .item(item)
                .conditionExpression(
                        String.format(
                                "attribute_not_exists(%s)", DynamoDBTableEntryConstants.TABLE_ID))
                .build();
        client.putItem(request);

        Map<String, String> tableConf = new HashMap();
        tableConf.put(DynamoDBTableEntryConstants.TABLE_ID, tableId);

        return tableConf;
    }

    // Copied from DynamoDbLogStore. TODO: add the logging back.

    /**
     * Ensures that the table used to store commits from all Delta tables exists. If the table
     * does not exist, it will be created.
     * @throws IOException
     */
    private void tryEnsureTableExists() throws IOException {
        int retries = 0;
        boolean created = false;
        while(retries < 20) {
            String status = "CREATING";
            try {
                DescribeTableResponse result = client.describeTable(
                        DescribeTableRequest.builder()
                                .tableName(coordinatedCommitsTableName)
                                .build());
                TableDescription descr = result.table();
                status = descr.tableStatusAsString();
            } catch (ResourceNotFoundException e) {
                LOG.info(
                        "DynamoDB table `{}` for endpoint `{}` does not exist. " +
                        "Creating it now with provisioned throughput of {} RCUs and {} WCUs.",
                        coordinatedCommitsTableName, endpoint, readCapacityUnits, writeCapacityUnits);
                try {
                    CreateTableRequest request = CreateTableRequest.builder()
                            .attributeDefinitions(AttributeDefinition.builder()
                                    .attributeName(DynamoDBTableEntryConstants.TABLE_ID)
                                    .attributeType(ScalarAttributeType.S)
                                    .build())
                            .tableName(coordinatedCommitsTableName)
                            .keySchema(KeySchemaElement.builder()
                                    .attributeName(DynamoDBTableEntryConstants.TABLE_ID)
                                    .keyType(KeyType.HASH)
                                    .build())
                            .provisionedThroughput(ProvisionedThroughput.builder()
                                    .readCapacityUnits(this.readCapacityUnits)
                                    .writeCapacityUnits(this.writeCapacityUnits)
                                    .build())
                            .build();
                    client.createTable(request);
                    created = true;
                } catch (ResourceInUseException e3) {
                    // race condition - table just created by concurrent process
                }
            }
            if (status.equals("ACTIVE")) {
                if (created) {
                    LOG.info("Successfully created DynamoDB table `{}`", coordinatedCommitsTableName);
                } else {
                    LOG.info("Table `{}` already exists", coordinatedCommitsTableName);
                }
                break;
            } else if (status.equals("CREATING")) {
                retries += 1;
                LOG.info("Waiting for `{}` table creation", coordinatedCommitsTableName);
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) {
                    throw new InterruptedIOException(e.getMessage());
                }
            } else {
                LOG.error("table `{}` status: {}", coordinatedCommitsTableName, status);
                throw new RuntimeException("DynamoDBCommitCoordinatorCliet: Unable to create table with " +
                        "name " + coordinatedCommitsTableName + " for endpoint " + endpoint + ". Ensure " +
                        "that the credentials provided have the necessary permissions to create " +
                        "tables in DynamoDB. If the table already exists, ensure that the table " +
                        "is in the ACTIVE state.");
            }
        };
    }

    @Override
    public boolean semanticEquals(CommitCoordinatorClient other) {
        if (!(other instanceof DynamoDBCommitCoordinatorClient)) {
            return false;
        }
        DynamoDBCommitCoordinatorClient otherStore = (DynamoDBCommitCoordinatorClient) other;
        return this.coordinatedCommitsTableName.equals(otherStore.coordinatedCommitsTableName)
                && this.endpoint.equals(otherStore.endpoint);
    }
}
