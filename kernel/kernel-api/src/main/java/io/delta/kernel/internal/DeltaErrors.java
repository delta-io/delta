/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import java.sql.Timestamp;
import java.util.Optional;

import io.delta.kernel.expressions.Expression;

public final class DeltaErrors {
    private DeltaErrors() {}

    // TODO update to be user-facing exception with future exception framework
    //  (see delta-io/delta#2231) & document in method docs as needed (Table::getSnapshotAtVersion)
    public static RuntimeException nonReconstructableStateException(
            String tablePath, long version) {
        String message = String.format(
            "%s: Unable to reconstruct state at version %s as the transaction log has been " +
                "truncated due to manual deletion or the log retention policy and checkpoint " +
                "retention policy.",
            tablePath,
            version);
        return new RuntimeException(message);
    }

    // TODO update to be user-facing exception with future exception framework
    //  (see delta-io/delta#2231) & document in method docs as needed (Table::getSnapshotAtVersion)
    public static RuntimeException nonExistentVersionException(
            String tablePath, long versionToLoad, long latestVersion) {
        String message = String.format(
            "%s: Trying to load a non-existent version %s. The latest version available is %s",
            tablePath,
            versionToLoad,
            latestVersion);
        return new RuntimeException(message);
    }

    // TODO update to be user-facing exception with future exception framework
    //  (see delta-io/delta#2231) & document in method docs as needed
    //  (Table::getSnapshotAtTimestamp)
    public static RuntimeException timestampEarlierThanTableFirstCommitException(
            String tablePath, long providedTimestamp, long commitTimestamp) {
        String message = String.format(
            "%s: The provided timestamp %s ms (%s) is before the earliest version available. " +
                "Please use a timestamp greater than or equal to %s ms (%s)",
            tablePath,
            providedTimestamp,
            formatTimestamp(providedTimestamp),
            commitTimestamp,
            formatTimestamp(commitTimestamp));
        return new RuntimeException(message);
    }

    // TODO update to be user-facing exception with future exception framework
    //  (see delta-io/delta#2231) & document in method docs as needed
    //  (Table::getSnapshotAtTimestamp)
    public static RuntimeException timestampLaterThanTableLastCommit(
            String tablePath, long providedTimestamp, long commitTimestamp, long commitVersion) {
        String commitTimestampStr = formatTimestamp(commitTimestamp);
        String message = String.format(
            "%s: The provided timestamp %s ms (%s) is after the latest commit with " +
                "timestamp %s ms (%s). If you wish to query this version of the table please " +
                "either provide the version %s or use the exact timestamp of the last " +
                "commit %s ms (%s)",
            tablePath,
            providedTimestamp,
            formatTimestamp(providedTimestamp),
            commitTimestamp,
            commitTimestampStr,
            commitVersion,
            commitTimestamp,
            commitTimestampStr);
        return new RuntimeException(message);
    }

    // TODO: Change the exception to proper type as part of the exception framework
    // (see delta-io/delta#2231)
    /**
     * Exception thrown when the expression evaluator doesn't support the given expression.
     * @param expression
     * @param reason Optional additional reason for why the expression is not supported.
     * @return
     */
    public static UnsupportedOperationException unsupportedExpression(
            Expression expression,
            Optional<String> reason) {
        String message = String.format(
            "Expression evaluator doesn't support the expression: %s.%s",
                expression,
                reason.map(r -> " Reason: " + r).orElse(""));
        return new UnsupportedOperationException(message);
    }

    public static UnsupportedOperationException unsupportedReaderProtocol(int readVersion) {
        throw new UnsupportedOperationException(
                "Unsupported reader protocol version: " + readVersion);
    }

    public static UnsupportedOperationException unsupportedReadFeature(
            int readProtocolVersion,
            String readFeature) {
        throw new UnsupportedOperationException(String.format(
                "Unsupported reader protocol version: %s with feature: %s",
                    readProtocolVersion, readFeature));
    }

    public static UnsupportedOperationException unsupportedWriterProtocol(int writeVersion) {
        throw new UnsupportedOperationException(
                "Unsupported writer protocol version: " + writeVersion);
    }

    public static UnsupportedOperationException unsupportedWriteFeature(
            int writeProtocolVersion,
            String writeFeature) {
        throw new UnsupportedOperationException(String.format(
                "Unsupported writer protocol version: %s with feature: %s",
                writeProtocolVersion, writeFeature));
    }

    private static String formatTimestamp(long millisSinceEpochUTC) {
        return new Timestamp(millisSinceEpochUTC).toInstant().toString();
    }
}
