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

/**
 * Defines the field names used in the DynamoDB table entry.
 */
final class DynamoDBTableEntryConstants {
    private DynamoDBTableEntryConstants() {}

    /** The primary key of the DynamoDB table. */
    public static final String TABLE_ID = "tableId";
    /** The version of the latest commit in the corresponding Delta table. */
    public static final String TABLE_LATEST_VERSION = "tableVersion";
    /** The inCommitTimestamp of the latest commit in the corresponding Delta table. */
    public static final String TABLE_LATEST_TIMESTAMP = "tableTimestamp";
    /** Whether this commit coordinator is accepting more commits for the corresponding Delta table. */
    public static final String ACCEPTING_COMMITS = "acceptingCommits";
    /** The path of the corresponding Delta table. */
    public static final String TABLE_PATH = "path";
    /** The schema version of this DynamoDB table entry. */
    public static final String SCHEMA_VERSION = "schemaVersion";
    /**
     * Whether this commit coordinator has accepted any commits after `registerTable`.
     */
    public static final String HAS_ACCEPTED_COMMITS = "hasAcceptedCommits";
    /** The name of the field used to store unbackfilled commits. */
    public static final String COMMITS = "commits";
    /** The unbackfilled commit version. */
    public static final String COMMIT_VERSION = "version";
    /** The inCommitTimestamp of the unbackfilled commit. */
    public static final String COMMIT_TIMESTAMP = "timestamp";
    /** The name of the unbackfilled file. e.g. 00001.uuid.json */
    public static final String COMMIT_FILE_NAME = "fsName";
    /** The length of the unbackfilled file as per the file status. */
    public static final String COMMIT_FILE_LENGTH = "fsLength";
    /** The modification timestamp of the unbackfilled file as per the file status. */
    public static final String COMMIT_FILE_MODIFICATION_TIMESTAMP = "fsTimestamp";
}
