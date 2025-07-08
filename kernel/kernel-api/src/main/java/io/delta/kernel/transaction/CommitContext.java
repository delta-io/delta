package io.delta.kernel.transaction;

import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;

/**
 * <pre>{@code
 * // Use Case 1: No action materialization
 * CloseableIterator<Row> allDataActionsIter = ... // Collect from executors.
 * CommitContext context = txn.getInitialCommitContext(engine, allDataActionsIter)
 * resolvedTable.getCommitter.commit(
 *   engine,
 *   context.getFinalizedActions, // iterator
 *   context.getCommitMetadata() // wrapper of POJOs, for now
 * );
 *
 * // Use case 2: Materialize actions and retry
 * CloseableIterator<Row> allDataActionsIter = ... // Collect from executors.
 * CommitContext context = txn.getInitialCommitContext(engine, allDataActionsIter)
 *
 * // Zach: why not just resolvedTable.commit(...)
 *
 * while (numRetries < MAX_RETRIES) {
 *   List<Row> finalizedActionsList = customEngineMaterializer(context.getFinalizedActions())
 *   try {
 *     resolvedTable.getCommitter.commit(
 *       engine,
 *       // if we passed in the commitContext --> that iterator has already been consumed! hasNext is false
 *       finalizedActionsList.iterator(),
 *       context.getCommitMetadata()
 *     );
 *   } catch (CommitFailedException e) {
 *     // e.g. UC.getCommits() // to get the conflicting ratified commits
 *     context = txn.resolveConflictsAndRebase(
 *       finalizedActionsList.iterator(), // Is there any better contract to ensure these are the same?
 *       context,
 *       winningCatalogCommits,
 *       catalogMaxRatifiedVersion
 *     );
 *   }
 * }
 * }</pre>
 */
public interface CommitContext {
  CloseableIterator<Row> getFinalizedActions();

  CommitMetadata getCommitMetadata();
}
