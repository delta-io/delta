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

package io.delta.standalone;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.expressions.Expression;

/**
 * Used to perform a set of reads in a transaction and then commit a set of updates to the
 * state of the log.  All reads from the {@link DeltaLog}, MUST go through this instance rather
 * than directly to the {@link DeltaLog} otherwise they will not be checked for logical conflicts
 * with concurrent updates.
 *
 * This class is not thread-safe.
 */
public interface OptimisticTransaction {

    /**
     * Modifies the state of the log by adding a new commit that is based on a read at
     * the given `lastVersion`. In the case of a conflict with a concurrent writer this
     * method will throw an exception.
     *
     * @param <T>  A derived class of {@link Action}. This allows, for example, both a
     *             {@code List<Action>} and a {@code List<AddFile>} to be accepted.
     * @param actions  Set of actions to commit.
     * @param op  Details of operation that is performing this transactional commit.
     * @param engineInfo  String used to identify the writer engine. It should resemble
     *                 "{engineName}/{engineVersion}".
     * @return a {@link CommitResult}, wrapping the table version that was committed.
     */
    <T extends Action> CommitResult commit(Iterable<T> actions, Operation op, String engineInfo);

    /**
     * Mark files matched by the `readPredicates` as read by this transaction.
     *
     * Internally, the `readPredicates` parameter and the resultant `readFiles` will be used to
     * determine if logical conflicts between this transaction and previously-committed transactions
     * can be resolved (i.e. no error thrown).
     *
     * For example:
     * - This transaction TXN1 reads partition 'date=2021-09-08' to perform an UPDATE and tries to
     *   commit at the next table version N.
     * - After TXN1 starts, another transaction TXN2 reads partition 'date=2021-09-07' and commits
     *   first at table version N (with no other metadata changes).
     * - TXN1 sees that another commit won, and needs to know whether to commit at version N+1 or
     *   fail. Using the `readPredicate` and resultant `readFiles`, TXN1 can see that none of its
     *   read files were changed by TXN2. Thus there are no logical conflicts and TXN1 can commit at
     *   table version N+1.
     *
     * @param readPredicate  Predicate used to determine which files were read.
     * @return a {@link DeltaScan} containing the list of files matching the push portion of the
     *         readPredicate.
     */
    DeltaScan markFilesAsRead(Expression readPredicate);

    /**
     * Records an update to the metadata that should be committed with this transaction.
     * Note that this must be done before writing out any files so that file writing
     * and checks happen with the final metadata for the table.
     *
     * IMPORTANT: It is the responsibility of the caller to ensure that files currently
     * present in the table are still valid under the new metadata.
     *
     * @param metadata  The new metadata for the delta table.
     */
    void updateMetadata(Metadata metadata);

    /**
     * Mark the entire table as tainted (i.e. read) by this transaction.
     */
    void readWholeTable();

    /**
     * @param id  transaction id
     * @return the latest version that has committed for the idempotent transaction with given `id`.
     */
    long txnVersion(String id);

    /**
     * @return the metadata for this transaction. The metadata refers to the metadata of the
     *         snapshot at the transaction's read version unless updated during the transaction.
     */
    Metadata metadata();
}
