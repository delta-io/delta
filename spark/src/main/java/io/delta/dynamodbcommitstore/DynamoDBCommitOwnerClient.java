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

package io.delta.dynamodbcommitstore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.spark.sql.delta.managedcommit.*;
import org.apache.spark.sql.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Map;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;

// TODO(dynamodb):
//  1. figure out whether we need retry logic like DeltaCommitClient
//  ~~2. data schema versioning~~
//  3. logging
//  4. error handling
//  5. local tests
//  6. passing hadoop conf to the credential provider

/**
 * A commit owner client that uses DynamoDB as the commit owner. The table schema is as follows:
 * tableId: String --- The unique identifier for the table. This is a UUID.
 * path: String --- The fully qualified path of the table in the file system. e.g. s3://bucket/path.
 * acceptingCommits: Boolean --- Whether the commit owner is accepting new commits. This will only
 *  be set to false when the table is converted from managed commits to file system commits.
 * tableVersion: Number --- The version of the latest commit.
 * tableTimestamp: Number --- The inCommitTimestamp of the latest commit.
 * schemaVersion: Number --- The version of the schema used to store the data.
 * commits: --- The list of unbackfilled commits.
 *  version: Number --- The version of the commit.
 *  inCommitTimestamp: Number --- The inCommitTimestamp of the commit.
 *  fsName: String --- The name of the unbackfilled file.
 *  fsLength: Number --- The length of the unbackfilled file.
 *  fsTimestamp: Number --- The modification time of the unbackfilled file.
 */
public class DynamoDBCommitOwnerClient implements CommitOwnerClient {

    /**
     * The name of the DynamoDB table used to store unbackfilled commits.
     */
    final String managedCommitsTableName;

    /**
     * The DynamoDB client used to interact with the DynamoDB table.
     */
    final AmazonDynamoDB client;

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
     * The key used to store the tableId in the managed commit table configuration.
     */
    final static String TABLE_CONF_TABLE_ID_KEY = "tableId";

    /**
     * The version of the client. This is used to ensure that the client is compatible with the
     * schema of the data stored in the DynamoDB table. A client should only be able to
     * access a table if the schema version of the table matches the client version.
     */
    final int CLIENT_VERSION = 1;


    public DynamoDBCommitOwnerClient(
            String managedCommitsTableName,
            String endpoint,
            AmazonDynamoDB client,
            long backfillBatchSize) throws IOException {
        this(
            managedCommitsTableName,
            endpoint,
            client,
            backfillBatchSize,
            5 /* readCapacityUnits */,
            5 /* writeCapacityUnits */,
            false /* skipPathCheck */);
    }

    public DynamoDBCommitOwnerClient(
            String managedCommitsTableName,
            String endpoint,
            AmazonDynamoDB client,
            long backfillBatchSize,
            long readCapacityUnits,
            long writeCapacityUnits,
            boolean skipPathCheck) throws IOException {
        this.managedCommitsTableName = managedCommitsTableName;
        this.endpoint = endpoint;
        this.client = client;
        this.backfillBatchSize = backfillBatchSize;
        this.readCapacityUnits = readCapacityUnits;
        this.writeCapacityUnits = writeCapacityUnits;
        this.skipPathCheck = skipPathCheck;
        tryEnsureTableExists();
    }

    private String getTableId(Map<String, String> managedCommitTableConf) {
        return managedCommitTableConf.get(TABLE_CONF_TABLE_ID_KEY).getOrElse(() -> {
            throw new RuntimeException("tableId not found");
        });
    }

    /**
     * Fetches the entry from the commit owner for the given table. Only the attributes defined
     * in attributesToGet will be fetched.
     */
    private GetItemResult getEntryFromCommitOwner(
            Map<String, String> managedCommitTableConf, String... attributesToGet) {
        GetItemRequest request = new GetItemRequest()
                .withTableName(managedCommitsTableName)
                .addKeyEntry(
                        DynamoDBTableEntryConstants.TABLE_ID,
                        new AttributeValue().withS(getTableId(managedCommitTableConf)))
                .withAttributesToGet(attributesToGet);
        return client.getItem(request);
    }

    /**
     * Commits the given file to the commit owner.
     * A conditional write is performed to the DynamoDB table entry associated with this Delta
     * table.
     * If the conditional write goes through, the filestatus of the UUID delta file will be
     * appended to the list of unbackfilled commits, and other updates like setting the latest
     * table version to `attemptVersion` will be performed.
     *
     * For the conditional write to go through, the following conditions must be met right before
     * the write is performed:
     * 1. The latest table version in DynamoDB is equal to attemptVersion - 1.
     * 2. The commit owner is accepting new commits.
     * 3. The schema version of the commit owner matches the schema version of the client.
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
    protected CommitResponse commitToOwner(
            Path logPath,
            Map<String, String> managedCommitTableConf,
            long attemptVersion,
            FileStatus commitFile,
            long inCommitTimestamp,
            boolean isMCtoFSConversion) throws CommitFailedException {
        // Add conditions for the conditional update.
        java.util.Map<String, ExpectedAttributeValue> expectedValuesBeforeUpdate = new HashMap<>();
        expectedValuesBeforeUpdate.put(
                DynamoDBTableEntryConstants.TABLE_LATEST_VERSION,
                new ExpectedAttributeValue()
                        .withValue(new AttributeValue().withN(Long.toString(attemptVersion - 1)))
        );
        expectedValuesBeforeUpdate.put(
                DynamoDBTableEntryConstants.ACCEPTING_COMMITS,
                new ExpectedAttributeValue()
                    .withValue(new AttributeValue().withBOOL(true)));
        if (!skipPathCheck) {
            expectedValuesBeforeUpdate.put(
                    DynamoDBTableEntryConstants.TABLE_PATH,
                    new ExpectedAttributeValue()
                        .withValue(new AttributeValue().withS(logPath.getParent().toString())));
        }
        expectedValuesBeforeUpdate.put(
                DynamoDBTableEntryConstants.SCHEMA_VERSION,
                new ExpectedAttributeValue()
                    .withValue(new AttributeValue().withN(Integer.toString(CLIENT_VERSION))));

        java.util.Map<String, AttributeValue> newCommit = new HashMap<>();
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_VERSION,
                new AttributeValue().withN(Long.toString(attemptVersion)));
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_TIMESTAMP,
                new AttributeValue().withN(Long.toString(inCommitTimestamp)));
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_FILE_NAME,
                new AttributeValue().withS(commitFile.getPath().getName()));
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_FILE_LENGTH,
                new AttributeValue().withN(Long.toString(commitFile.getLen())));
        newCommit.put(
                DynamoDBTableEntryConstants.COMMIT_FILE_MODIFICATION_TIMESTAMP,
                new AttributeValue().withN(Long.toString(commitFile.getModificationTime())));

        UpdateItemRequest request = new UpdateItemRequest()
                .withTableName(managedCommitsTableName)
                .addKeyEntry(
                        DynamoDBTableEntryConstants.TABLE_ID,
                        new AttributeValue().withS(getTableId(managedCommitTableConf)))
                .addAttributeUpdatesEntry(
                        DynamoDBTableEntryConstants.TABLE_LATEST_VERSION, new AttributeValueUpdate()
                            .withValue(new AttributeValue().withN(Long.toString(attemptVersion)))
                            .withAction(AttributeAction.PUT))
                .addAttributeUpdatesEntry(
                        DynamoDBTableEntryConstants.TABLE_LATEST_TIMESTAMP, new AttributeValueUpdate()
                            .withValue(new AttributeValue().withN(Long.toString(inCommitTimestamp)))
                            .withAction(AttributeAction.PUT))
                .addAttributeUpdatesEntry(
                        DynamoDBTableEntryConstants.COMMITS,
                        new AttributeValueUpdate()
                            .withAction(AttributeAction.ADD)
                            .withValue(new AttributeValue().withL(
                                    new AttributeValue().withM(newCommit)
                                )
                            )
                )
                .withExpected(expectedValuesBeforeUpdate);

        if (isMCtoFSConversion) {
            // If this table is being converted from managed commits to file system commits, we need
            // to set acceptingCommits to false.
            request = request
                    .addAttributeUpdatesEntry(
                            DynamoDBTableEntryConstants.ACCEPTING_COMMITS,
                            new AttributeValueUpdate()
                                    .withValue(new AttributeValue().withBOOL(false))
                                    .withAction(AttributeAction.PUT)
                    );
        }

        try {
            client.updateItem(request);
        } catch (ConditionalCheckFailedException e) {
            // Conditional check failed. The exception will not indicate which condition failed.
            // We need to check the conditions ourselves by fetching the item and checking the
            // values.
            GetItemResult latestEntry = getEntryFromCommitOwner(
                    managedCommitTableConf,
                    DynamoDBTableEntryConstants.TABLE_LATEST_VERSION,
                    DynamoDBTableEntryConstants.ACCEPTING_COMMITS,
                    DynamoDBTableEntryConstants.TABLE_PATH,
                    DynamoDBTableEntryConstants.SCHEMA_VERSION);

            int schemaVersion = Integer.parseInt(
                    latestEntry.getItem().get(DynamoDBTableEntryConstants.SCHEMA_VERSION).getN());
            if (schemaVersion != CLIENT_VERSION) {
                throw new CommitFailedException(
                        false /* retryable */,
                        false /* conflict */,
                        "The schema version of the commit owner does not match the current" +
                                "DynamoDBCommitOwnerClient version. The data schema version is " +
                                " " + schemaVersion + " while the client version is " +
                                CLIENT_VERSION + ". Make sure that the correct client is being " +
                                "used to access this table." );
            }
            long latestTableVersion = Long.parseLong(
                    latestEntry.getItem().get(DynamoDBTableEntryConstants.TABLE_LATEST_VERSION).getN());
            if (!skipPathCheck &&
                    !latestEntry.getItem().get("path").getS().equals(logPath.getParent().toString())) {
                throw new CommitFailedException(
                        false /* retryable */,
                        false /* conflict */,
                        "This commit was attempted from path " + logPath.getParent() +
                                " while the table is registered at " +
                                latestEntry.getItem().get("path").getS() + ".");
            }
            if (!latestEntry.getItem().get(DynamoDBTableEntryConstants.ACCEPTING_COMMITS).getBOOL()) {
                throw new CommitFailedException(
                        false /* retryable */,
                        false /* conflict */,
                        "The commit owner is not accepting any new commits for this table.");
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
            Path logPath,
            Map<String, String> managedCommitTableConf,
            long commitVersion,
            Iterator<String> actions,
            UpdatedActions updatedActions) {
        if (commitVersion == 0) {
            throw new CommitFailedException(
                    false /* retryable */,
                    false /* conflict */,
                    "Commit version 0 must go via filesystem.");
        }
        try {
            FileSystem fs = logPath.getFileSystem(hadoopConf);
            Path commitPath =
                    ManagedCommitUtils.generateUnbackfilledDeltaFilePath(logPath, commitVersion);
            logStore.write(commitPath, actions, true /* overwrite */, hadoopConf);
            FileStatus commitFileStatus = fs.getFileStatus(commitPath);
            long inCommitTimestamp = updatedActions.getCommitInfo().getCommitTimestamp();
            boolean isMCtoFSConversion =
                    ManagedCommitUtils.isManagedCommitToFSConversion(commitVersion, updatedActions);
            CommitResponse res = commitToOwner(
                    logPath,
                    managedCommitTableConf,
                    commitVersion,
                    commitFileStatus,
                    inCommitTimestamp,
                    isMCtoFSConversion);

            boolean shouldBackfillOnEveryCommit = backfillBatchSize <= 1;
            boolean isBatchBackfillDue = commitVersion % backfillBatchSize == 0;
            boolean shouldBackfill = shouldBackfillOnEveryCommit || isBatchBackfillDue ||
                    // Always attempt a backfill for managed commit to filesystem conversion.
                    // Even if this fails, the next reader will attempt to backfill.
                    isMCtoFSConversion;
            if (shouldBackfill) {
                backfillToVersion(
                    logStore,
                    hadoopConf,
                    logPath,
                    managedCommitTableConf,
                    commitVersion,
                    Option.empty());
            }
            return res;
        } catch (IOException e) {
            throw new CommitFailedException(false /* retryable */, false /* conflict */, e.getMessage());
        }
    }

    private GetCommitsResponse getCommitsImpl(
            Path logPath,
            Map<String, String> tableConf,
            Option<Object> startVersion,
            Option<Object> endVersion) throws IOException {
        GetItemResult latestEntry = getEntryFromCommitOwner(
                tableConf,
                DynamoDBTableEntryConstants.COMMITS,
                DynamoDBTableEntryConstants.TABLE_LATEST_VERSION);

        java.util.Map<String, AttributeValue> item = latestEntry.getItem();
        long currentVersion =
                Long.parseLong(item.get(DynamoDBTableEntryConstants.TABLE_LATEST_VERSION).getN());
        AttributeValue allStoredCommits = item.get(DynamoDBTableEntryConstants.COMMITS);
        ArrayList<Commit> commits = new ArrayList<>();
        Path unbackfilledCommitsPath = new Path(logPath, ManagedCommitUtils.COMMIT_SUBDIR);
        for(AttributeValue attr: allStoredCommits.getL()) {
            java.util.Map<String, AttributeValue> commitMap = attr.getM();
            long commitVersion =
                    Long.parseLong(commitMap.get(DynamoDBTableEntryConstants.COMMIT_VERSION).getN());
            boolean commitInRange = startVersion.forall((start) -> commitVersion >= (long) start) &&
                    endVersion.forall((end) -> (long) end >= commitVersion);
            if (commitInRange) {
                Path filePath = new Path(
                        unbackfilledCommitsPath,
                        commitMap.get(DynamoDBTableEntryConstants.COMMIT_FILE_NAME).getS());
                long length =
                        Long.parseLong(commitMap.get(DynamoDBTableEntryConstants.COMMIT_FILE_LENGTH).getN());
                long modificationTime = Long.parseLong(
                        commitMap.get(DynamoDBTableEntryConstants.COMMIT_FILE_MODIFICATION_TIMESTAMP).getN());
                FileStatus fileStatus = new FileStatus(
                        length,
                        false /* isDir */,
                        0 /* blockReplication */,
                        0 /* blockSize */,
                        modificationTime,
                        filePath);
                long inCommitTimestamp =
                        Long.parseLong(commitMap.get(DynamoDBTableEntryConstants.COMMIT_TIMESTAMP).getN());
                commits.add(new Commit(commitVersion, fileStatus, inCommitTimestamp));
            }
        }
        return new GetCommitsResponse(
                JavaConverters.asScalaIterator(commits.iterator()).toSeq(), currentVersion);
    }

    @Override
    public GetCommitsResponse getCommits(
            Path logPath,
            Map<String, String> managedCommitTableConf,
            Option<Object> startVersion,
            Option<Object> endVersion) {
        try {
            return getCommitsImpl(logPath, managedCommitTableConf, startVersion, endVersion);
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
    private void writeActionsToFile(
            LogStore logStore,
            Path logPath,
            long version,
            scala.collection.Iterator<String> actions,
            Configuration hadoopConf,
            boolean shouldOverwrite) throws FileAlreadyExistsException {
        Path targetPath = ManagedCommitUtils.getBackfilledDeltaFilePath(logPath, version);
        logStore.write(targetPath, actions, shouldOverwrite, hadoopConf);
    }

    private void validateBackfilledFileExists(
            Path logPath, Configuration hadoopConf, Option<Object> lastKnownBackfilledVersion) {
        try {
            if (lastKnownBackfilledVersion.isEmpty()) {
                return;
            }
            long version = (long) lastKnownBackfilledVersion.get();
            Path lastKnownBackfilledFile =
                    ManagedCommitUtils.getBackfilledDeltaFilePath(logPath, version);
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
     * Backfills all the unbackfilled commits returned by the commit owner and notifies the commit
     * owner of the backfills.
     * The version parameter is ignored in this implementation and all the unbackfilled commits
     * are backfilled. This method will not throw any exception if the physical backfill
     * succeeds but the update to the commit owner fails.
     * @throws IllegalArgumentException if the requested backfill version is greater than the latest
     *  version for the table.
     */
    @Override
    public void backfillToVersion(
            LogStore logStore,
            Configuration hadoopConf,
            Path logPath,
            Map<String, String> managedCommitTableConf,
            long version,
            Option<Object> lastKnownBackfilledVersion) {
        GetCommitsResponse resp =
                getCommits(logPath, managedCommitTableConf, lastKnownBackfilledVersion, Option.empty());
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
        for (Commit commit: JavaConverters.asJavaIterable(resp.getCommits())) {
            scala.collection.Iterator<String> actions =
                    logStore.read(commit.getFileStatus().getPath(), hadoopConf).toIterator();
            try {
                writeActionsToFile(
                        logStore,
                        logPath,
                        commit.getVersion(),
                        actions,
                        hadoopConf,
                        shouldOverwrite);
            } catch (java.nio.file.FileAlreadyExistsException e) {
                // Ignore the exception. This indicates that the file has already been backfilled.
            }
        }
        UpdateItemRequest request = new UpdateItemRequest()
                .withTableName(managedCommitsTableName)
                .addKeyEntry(
                        DynamoDBTableEntryConstants.TABLE_ID,
                        new AttributeValue().withS(getTableId(managedCommitTableConf)))
                .addAttributeUpdatesEntry(
                        DynamoDBTableEntryConstants.COMMITS,
                        new AttributeValueUpdate()
                            .withAction(AttributeAction.PUT)
                            .withValue(new AttributeValue().withL())
                )
                .withExpected(new HashMap<String, ExpectedAttributeValue>(){
                    {
                        put(DynamoDBTableEntryConstants.TABLE_LATEST_VERSION, new ExpectedAttributeValue()
                                .withValue(
                                        new AttributeValue()
                                                .withN(Long.toString(resp.getLatestTableVersion())))
                        );
                        put(DynamoDBTableEntryConstants.TABLE_PATH, new ExpectedAttributeValue()
                                .withValue(
                                        new AttributeValue()
                                                .withS(logPath.getParent().toString()))
                        );
                        put(DynamoDBTableEntryConstants.SCHEMA_VERSION, new ExpectedAttributeValue()
                                .withValue(
                                        new AttributeValue()
                                                .withN(Integer.toString(CLIENT_VERSION)))
                        );
                    }
                });
        try {
            client.updateItem(request);
        } catch (ConditionalCheckFailedException e) {
            // Ignore the exception. The backfill succeeded but the update to
            // the commit owner failed. The main purpose of a backfill operation is to ensure that
            // UUID commit is physically copied to a standard commit file path. A failed update to
            // the commit owner is not critical.
        }
    }

    @Override
    public Map<String, String> registerTable(
            Path logPath,
            long currentVersion,
            AbstractMetadata currentMetadata,
            AbstractProtocol currentProtocol) {
        java.util.Map<String, AttributeValue> item = new HashMap<>();

        String tableId = java.util.UUID.randomUUID().toString();
        item.put(DynamoDBTableEntryConstants.TABLE_ID, new AttributeValue().withS(tableId));

        // We maintain the invariant that a commit will only succeed if the latestVersion stored
        // in the table is equal to attemptVersion - 1. To maintain this, even though the
        // filesystem-based commit after register table can fail, we still treat the attemptVersion
        // at registration as a valid version. It is expected that the Delta client will perform
        // another registration if the filesystem-based commit fails. When the filesystem-based
        // commit does fail, this entry will not be associated with any Delta table because the
        // `tableId` generated above must be committed as part of the filesystem-based commit for
        // this entry to be associated with a Delta table.
        long attemptVersion = currentVersion + 1;
        item.put(
                DynamoDBTableEntryConstants.TABLE_LATEST_VERSION,
                new AttributeValue().withN(Long.toString(attemptVersion)));

        item.put(
                DynamoDBTableEntryConstants.TABLE_PATH,
                new AttributeValue().withS(logPath.getParent().toString()));
        item.put(DynamoDBTableEntryConstants.COMMITS, new AttributeValue().withL());
        item.put(
                DynamoDBTableEntryConstants.ACCEPTING_COMMITS, new AttributeValue().withBOOL(true));
        item.put(
                DynamoDBTableEntryConstants.SCHEMA_VERSION,
                new AttributeValue().withN(Integer.toString(CLIENT_VERSION)));

        PutItemRequest request = new PutItemRequest()
                .withTableName(managedCommitsTableName)
                .withItem(item)
                .withConditionExpression(
                        String.format(
                                "attribute_not_exists(%s)", DynamoDBTableEntryConstants.TABLE_ID));
        client.putItem(request);

        Tuple2<String, String> tableIdEntry = new Tuple2<>(DynamoDBTableEntryConstants.TABLE_ID, tableId);
        Map<String, String> tableConf = Map$.MODULE$.<String, String>empty().$plus(tableIdEntry);

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
                DescribeTableResult result = client.describeTable(managedCommitsTableName);
                TableDescription descr = result.getTable();
                status = descr.getTableStatus();
            } catch (ResourceNotFoundException e) {
                try {
                    client.createTable(
                            // attributeDefinitions
                            java.util.Collections.singletonList(
                                    new AttributeDefinition(
                                            DynamoDBTableEntryConstants.TABLE_ID,
                                            ScalarAttributeType.S)
                            ),
                            managedCommitsTableName,
                            // keySchema
                            java.util.Collections.singletonList(
                                    new KeySchemaElement(
                                            DynamoDBTableEntryConstants.TABLE_ID,
                                            KeyType.HASH)
                            ),
                            new ProvisionedThroughput(this.readCapacityUnits, this.writeCapacityUnits)
                    );
                    created = true;
                } catch (ResourceInUseException e3) {
                    // race condition - table just created by concurrent process
                }
            }
            if (status.equals("ACTIVE")) {
                break;
            } else if (status.equals("CREATING")) {
                retries += 1;
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) {
                    throw new InterruptedIOException(e.getMessage());
                }
            } else {
                break;  // TODO - raise exception?
            }
        };
    }

    @Override
    public boolean semanticEquals(CommitOwnerClient other) {
        if (!(other instanceof DynamoDBCommitOwnerClient)) {
            return false;
        }
        DynamoDBCommitOwnerClient otherStore = (DynamoDBCommitOwnerClient) other;
        return this.managedCommitsTableName.equals(otherStore.managedCommitsTableName)
                && this.endpoint.equals(otherStore.endpoint);
    }
}
