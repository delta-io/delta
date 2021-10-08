package io.delta.standalone;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.expressions.Expression;

import java.util.List;

public interface OptimisticTransaction {

    /**
     * Modifies the state of the log by adding a new commit that is based on a read at
     * the given `lastVersion`. In the case of a conflict with a concurrent writer this
     * method will throw an exception.
     *
     * @param actions  Set of actions to commit.
     * @param op  Details of operation that is performing this transactional commit.
     * @param engineInfo  String used to identify the writer engine. It should resemble
     *                 "{engineName}-{engineVersion}".
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
     *   fail. Using the `readPredicates` and resultant `readFiles`, TXN1 can see that none of its
     *   readFiles were changed by TXN2. Thus there are no logical conflicts and TXN1 can commit at
     *   table version N+1.
     *
     * @param readPredicate  Predicates used to determine which files were read.
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
     * @param id  TODO
     * @return the latest version that has committed for the idempotent transaction with given `id`.
     */
    long txnVersion(String id);

    /**
     * @return the metadata for this transaction. The metadata refers to the metadata of the snapshot
     *         at the transaction's read version unless updated during the transaction.
     */
    Metadata metadata();
}
