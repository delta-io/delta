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

package io.delta.storage.commit.uccommitcoordinator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.delta.coordinatedcommits.CatalogTrackedInfo;
import io.delta.storage.CloseableIterator;
import io.delta.storage.LogStore;
import io.delta.storage.commit.*;
import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.commit.uniform.UniformMetadata;
import io.delta.storage.internal.FileNameUtils;
import io.delta.storage.internal.LogStoreErrors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A commit coordinator client that uses unity-catalog as the commit coordinator.
 */
public class UCCommitCoordinatorClient implements CommitCoordinatorClient {
  // Fields for Delta REST API access
  private final String baseUri;
  private final io.unitycatalog.client.auth.TokenProvider tokenProvider;

  public UCCommitCoordinatorClient(Map<String, String> conf, UCClient ucClient) {
    this(conf, ucClient, null, null);
  }

  public UCCommitCoordinatorClient(
      Map<String, String> conf,
      UCClient ucClient,
      String baseUri,
      io.unitycatalog.client.auth.TokenProvider tokenProvider) {
    this.conf = conf;
    this.ucClient = ucClient;
    this.baseUri = baseUri;
    this.tokenProvider = tokenProvider;
  }

  /**
   * Logger for UCCommitCoordinatorClient class operations and diagnostics.
   */
  private static final Logger LOG = LoggerFactory.getLogger(UCCommitCoordinatorClient.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  // UC Protocol Version Control Constants
  /** Supported version for read operations in the Unity Catalog protocol. */
  private static final int SUPPORTED_READ_VERSION = 0;

  /** Supported version for write operations in the Unity Catalog protocol. */
  private static final int SUPPORTED_WRITE_VERSION = 0;

  /** Key used to identify the read version in protocol communications with the UC server. */
  private static final String READ_VERSION_KEY = "readVersion";

  /** Key used to identify the write version in protocol communications with the UC server. */
  private static final String WRITE_VERSION_KEY = "writeVersion";

  /**
   * Temporary kill switch for sending metadata updates through UC from the Spark path.
   * TODO(issue #6296): remove once metadata updates are supported end-to-end.
   */
  private static final boolean SHOULD_PASS_METADATA_TO_UC = false;

  // Unity Catalog Identifiers
  /**
   * Key for identifying Unity Catalog table ID in `delta.coordinatedCommits.tableConf{-preview}`.
   */
  final static public String UC_TABLE_ID_KEY = "io.unitycatalog.tableId";
  // Previously this key was ucTableId. It was later renamed.
  final static public String UC_TABLE_ID_KEY_OLD = "ucTableId";
  /** Spark DSv2 managed-location marker. This is connector-managed metadata, not a Delta property. */
  private static final String PROP_IS_MANAGED_LOCATION = "is_managed_location";

  /**
   * Key for identifying Unity Catalog metastore ID in
   * `delta.coordinatedCommits.commitCoordinatorConf{-preview}`.
   */
  final static public String UC_METASTORE_ID_KEY = "ucMetastoreId";

  // Backfill and Retry Configuration
  /**
   * Offset from current commit version for backfill listing optimization.
   * Used to prevent expensive listings from version 0.
   */
  public static int BACKFILL_LISTING_OFFSET = 100;

  /** Maximum number of retry attempts for transient errors. */
  protected static final int MAX_RETRIES_ON_TRANSIENT_ERROR = 15;

  /** Initial wait time in milliseconds before retrying after a transient error. */
  protected static final long TRANSIENT_ERROR_RETRY_INITIAL_WAIT_MS = 100;

  /** Maximum wait time in milliseconds between retries for transient errors. */
  protected static final long TRANSIENT_ERROR_RETRY_MAX_WAIT_MS = 1000 * 60; // 1 minute

  // Thread Pool Configuration
  /** Size of the thread pool for handling asynchronous operations. */
  static protected int THREAD_POOL_SIZE = 20;

  /**
   * Thread pool executor for handling asynchronous tasks like backfilling.
   * Configured with daemon threads and custom naming pattern.
   */
  private static final ThreadPoolExecutor asyncExecutor;

  // Static Initializer Block
  static {
    asyncExecutor = new ThreadPoolExecutor(
      THREAD_POOL_SIZE,
      THREAD_POOL_SIZE,
      60L,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue<>(Integer.MAX_VALUE),
      new ThreadFactory() {
        private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(@Nonnull Runnable r) {
          Thread t = defaultFactory.newThread(r);
          // Set the thread name to uc-commit-coordinator-pool-1-thread-1
          t.setName("uc-commit-coordinator-" + t.getName());
          t.setDaemon(true);
          return t;
        }
      });
    asyncExecutor.allowCoreThreadTimeOut(true);
  }

  // Instance Variables
  /** Unity Catalog client instance for interacting with UC services. */
  public final UCClient ucClient;

  /** Configuration map containing settings for the coordinator client. */
  public final Map<String, String> conf;

  /**
   * Runs a task asynchronously using the backfillThreadPool.
   *
   * @param task The task to be executed asynchronously
   * @return A Future representing pending completion of the task
   */
  protected<T> Future<T> executeAsync(Callable<T> task) {
    return asyncExecutor.submit(task);
  }

  protected String extractUCTableId(TableDescriptor tableDesc) {
    Map<String, String> tableConf = tableDesc.getTableConf();
    if (!tableConf.containsKey(UC_TABLE_ID_KEY)) {
      throw new IllegalStateException("UC Table ID not found in " + tableConf);
    }
    return tableConf.get(UC_TABLE_ID_KEY);
  }

  /**
   * For UC, table registration is a no-op because we already contacted UC during table
   * creation and that already obtained the necessary table config and added
   * it to the metadata (this is for performance reasons and ease of use). As a result,
   * this method only verifies that the metadata has been added correct and is present.
   * Otherwise, it throws an exception.
   */
  @Override
  public Map<String, String> registerTable(
      Path logPath,
      Optional<TableIdentifier> tableIdentifier,
      long currentVersion,
      AbstractMetadata currentMetadata,
      AbstractProtocol currentProtocol) {
    Map<String, String> tableConf = CoordinatedCommitsUtils.getTableConf(currentMetadata);
    checkVersionSupported(tableConf, false /* compareRead */);

    // The coordinatedCommitsTableConf must have been instantiated prior to this call
    // with the UC table ID.
    if (!tableConf.containsKey(UC_TABLE_ID_KEY)) {
      throw new IllegalStateException("Could not verify if the table is registered with the " +
        "UC commit coordinator because the table ID is missing from the table metadata.");
    }
    // The coordinatedCommitsCoordinatorConf must have been instantiated prior to this call
    // with the metastore ID of the metastore, which stores the table.
    if (!CoordinatedCommitsUtils.getCoordinatorConf(currentMetadata).containsKey(
        UC_METASTORE_ID_KEY)) {
      throw new IllegalStateException("Could not verify if the table is registered with the UC " +
        "commit coordinator because the metastore ID is missing from the table metadata.");
    }
    return tableConf;
  }

  /**
   * Find the last known backfilled version by doing a listing of the last
   * {@link #BACKFILL_LISTING_OFFSET} commits. If no backfilled commits are found
   * among those, a UC call is made to get the oldest tracked commit in UC.
   */
  public long getLastKnownBackfilledVersion(
      long commitVersion,
      Configuration hadoopConf,
      LogStore logStore,
      TableDescriptor tableDesc
  ) {
    Path logPath = tableDesc.getLogPath();
    long listFromVersion = Math.max(0, commitVersion - BACKFILL_LISTING_OFFSET);
    Optional<Long> lastKnownBackfilledVersion =
      listAndGetLastKnownBackfilledVersion(listFromVersion, logStore, hadoopConf, logPath);
    if (!lastKnownBackfilledVersion.isPresent()) {
      // In case we don't find anything in the last 100 commits (should not happen)
      // we go to UC to find the earliest commit it is tracking as the commit prior
      // to that must have been backfilled.
      recordDeltaEvent(
        UCCoordinatedCommitsUsageLogs.UC_LAST_KNOWN_BACKFILLED_VERSION_NOT_FOUND,
        new HashMap<String, Object>() {{
          put("commitVersion", commitVersion);
          put("conf", conf);
          put("listFromVersion", listFromVersion);
          put("tableConf", tableDesc.getTableConf());
        }},
        logPath.getParent()
      );
      long minVersion =
        getCommits(tableDesc, null, null)
          .getCommits()
          .stream()
          .min(Comparator.comparingLong(Commit::getVersion))
          .map(Commit::getVersion)
          .orElseThrow(() -> new IllegalStateException("Couldn't find any unbackfilled commit " +
            "for table at " + logPath + " at version " + commitVersion));
      lastKnownBackfilledVersion = listAndGetLastKnownBackfilledVersion(
        minVersion - 1, logStore, hadoopConf, logPath);
      if (!lastKnownBackfilledVersion.isPresent()) {
        throw new IllegalStateException("Couldn't find any backfilled commit for table at " +
          logPath + " at version " + commitVersion);
      }
    }
    return lastKnownBackfilledVersion.get();
  }

  protected Iterator<FileStatus> listFrom(
      LogStore logStore,
      long listFromVersion,
      Configuration hadoopConf,
      Path logPath) {
    Path listingPath = CoordinatedCommitsUtils.getBackfilledDeltaFilePath(logPath, listFromVersion);
    try {
      return logStore.listFrom(listingPath, hadoopConf);
    } catch (IOException e) {
      LOG.error("Failed to list files from {} due to: {}", listingPath, exceptionString(e));
      throw new IllegalStateException(e);
    }
  }

  protected Optional<Long> listAndGetLastKnownBackfilledVersion(
      long listFromVersion,
      LogStore logStore,
      Configuration hadoopConf,
      Path logPath) {
    Optional<Long> lastKnownBackfilledVersion = Optional.empty();
    Iterator<FileStatus> deltaLogFileIt =
      listFrom(logStore, listFromVersion, hadoopConf, logPath);
    while (deltaLogFileIt.hasNext()) {
      FileStatus fileStatus = deltaLogFileIt.next();
      if (FileNameUtils.isDeltaFile(fileStatus.getPath())) {
        lastKnownBackfilledVersion =
          Optional.of(FileNameUtils.deltaVersion(fileStatus.getPath()));
      }
    }
    return lastKnownBackfilledVersion;
  }

  @Override
  public CommitResponse commit(
      LogStore logStore,
      Configuration hadoopConf,
      TableDescriptor tableDesc,
      long commitVersion,
      Iterator<String> actions,
      UpdatedActions updatedActions) throws CommitFailedException {
    return commitImpl(
      logStore,
      hadoopConf,
      tableDesc,
      commitVersion,
      actions,
      CatalogTrackedInfo.EMPTY
      , updatedActions);
  }

  public CommitResponse commit(
      LogStore logStore,
      Configuration hadoopConf,
      TableDescriptor tableDesc,
      long commitVersion,
      Iterator<String> actions,
      CatalogTrackedInfo catalogTrackedInfo,
      UpdatedActions updatedActions) throws CommitFailedException {
        return commitImpl(
          logStore,
          hadoopConf,
          tableDesc,
          commitVersion,
          actions,
          catalogTrackedInfo,
          updatedActions);
    }

  /**
   * Commits the provided actions as the specified version. The steps are as follows.
   *
   * 1. Write the actions to a UUID-based commit file
   * 2. In parallel to 1. determine the last known backfilled version.
   *    If a backfill hint is provided, we verify that it exists via a single HEAD call. Otherwise,
   *    the last known backfilled version is determined via a listing.
   * 3. Send commit request to UC to commit the version and register backfills up to the
   *    found last known backfilled version.
   * 4. Backfill all unbackfilled commits (including the latest one made in this call)
   *    asynchronously.
   *    A getCommits call is made to UC to retrieve all currently unbackfilled commits.
   */
  protected CommitResponse commitImpl(
      LogStore logStore,
      Configuration hadoopConf,
      TableDescriptor tableDesc,
      long commitVersion,
      Iterator<String> actions,
      CatalogTrackedInfo catalogTrackedInfo,
      UpdatedActions updatedActions) throws CommitFailedException {
    Path logPath = tableDesc.getLogPath();
    Map<String, String> coordinatedCommitsTableConf = tableDesc.getTableConf();
    checkVersionSupported(coordinatedCommitsTableConf, false /* compareRead */);
    // Writes may also have to perform reads to determine the last known backfilled
    // version/the commits to backfill in case we don't have a backfill hint. To
    // prevent to write to succeed but then fail the read, we do the read protocol
    // version check here.
    checkVersionSupported(coordinatedCommitsTableConf, true /* compareRead */);

    if (commitVersion == 0) {
      throw new CommitFailedException(
        false /* retryable */,
        false /* conflict */,
        "Commit version 0 must go via filesystem.");
    }

    long startTimeMs = System.currentTimeMillis();
    Map<String, Object> eventData = new HashMap<>();
    eventData.put("commitVersion", commitVersion);
    eventData.put("coordinatedCommitsTableConf", coordinatedCommitsTableConf);
    eventData.put("updatedActions", updatedActions);

    BiConsumer<Optional<Throwable>, String> recordUsageLog = (exception, opType) -> {
      exception.ifPresent(throwable -> {
        eventData.put("exceptionClass", throwable.getClass().getName());
        eventData.put("exceptionString", exceptionString(throwable));
      });
      eventData.put("totalTimeTakenMs", System.currentTimeMillis() - startTimeMs);
      recordDeltaEvent(opType, eventData, logPath.getParent());
    };

    // After commit 0, the table ID must exist in UC
    String tableId = extractUCTableId(tableDesc);
    LOG.info("Attempting to commit version " + commitVersion + " to table " + tableId);

    // Asynchronously verify/retrieve the last known backfilled version
    // Using AtomicLong instead of Long because we need to update the value in the lambda
    // and "Variable used in lambda expression should be final or effectively final".
    AtomicLong timeSpentInGettingLastKnownBackfilledVersion =
      new AtomicLong(System.currentTimeMillis());
    Future<Long> lastKnownBackfilledVersionFuture;
    try {
      lastKnownBackfilledVersionFuture = executeAsync(() -> {
        long foundVersion = getLastKnownBackfilledVersion(
          commitVersion,
          hadoopConf,
          logStore,
          tableDesc);
        timeSpentInGettingLastKnownBackfilledVersion.getAndUpdate(start ->
          System.currentTimeMillis() - start);
        return foundVersion;
      });
    } catch (Exception e) {
      // Synchronously verify/retrieve last known backfilled version.
      LOG.warn("Error while submitting task to verify/retrieve last known backfilled version " +
        "due to: " + exceptionString(e) + ". Verifying/retrieving synchronously");
      recordUsageLog.accept(
        Optional.of(e),
        UCCoordinatedCommitsUsageLogs.UC_BACKFILL_VALIDATION_FALLBACK_TO_SYNC);
      long foundVersion = getLastKnownBackfilledVersion(
        commitVersion,
        hadoopConf,
        logStore,
        tableDesc);
      timeSpentInGettingLastKnownBackfilledVersion.getAndUpdate(start ->
        System.currentTimeMillis() - start);;
      lastKnownBackfilledVersionFuture = CompletableFuture.completedFuture(foundVersion);
    }

    // In parallel to verifying/getting the last known backfilled version, write the commit file.
    long writeStartTimeMs = System.currentTimeMillis();
    FileStatus commitFile;
    try {
      commitFile = CoordinatedCommitsUtils.writeUnbackfilledCommitFile(
        logStore,
        hadoopConf,
        logPath.toString(),
        commitVersion,
        actions,
        UUID.randomUUID().toString()
      );
    } catch (IOException e) {
      throw new CommitFailedException(
        true /* retryable */,
        false /* conflict */,
        "Failed to write commit file due to: " + e.getMessage(),
        e);
    }
    eventData.put("writeCommitFileTimeTakenMs", System.currentTimeMillis() - writeStartTimeMs);

    // Using AtomicLong instead of Long because we need to access the value in the lambda
    // and "Variable used in lambda expression should be final or effectively final".
    AtomicLong lastKnownBackfilledVersion = new AtomicLong();
    try {
      lastKnownBackfilledVersion.set(lastKnownBackfilledVersionFuture.get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    long commitTimestamp = updatedActions.getCommitInfo().getCommitTimestamp();
    boolean disown = isDisownCommit(
      updatedActions.getOldMetadata(),
      updatedActions.getNewMetadata());
    eventData.put("tableId", tableId);
    eventData.put("lastKnownBackfilledVersion", lastKnownBackfilledVersion.get());
    eventData.put("commitTimestamp", commitTimestamp);
    eventData.put("disown", disown);
    eventData.put(
      "timeSpentInGettingLastKnownBackfilledVersion",
      timeSpentInGettingLastKnownBackfilledVersion);

    int transientErrorRetryCount = 0;
    while (transientErrorRetryCount <= MAX_RETRIES_ON_TRANSIENT_ERROR) {
      try {
        boolean shouldPassMetadata = shouldUseDeltaRestApi(tableDesc) || SHOULD_PASS_METADATA_TO_UC;
        commitToUC(
          tableDesc,
          logPath,
          Optional.of(commitFile),
          Optional.of(commitVersion),
          Optional.of(commitTimestamp),
          Optional.of(lastKnownBackfilledVersion.get()),
          catalogTrackedInfo,
          disown,
          updatedActions.getNewMetadata() == updatedActions.getOldMetadata() || !shouldPassMetadata ?
            Optional.empty() :
            Optional.of(updatedActions.getNewMetadata()),
          updatedActions.getNewProtocol() == updatedActions.getOldProtocol() ?
            Optional.empty() :
            Optional.of(updatedActions.getNewProtocol())
        );
        break;
      } catch (CommitFailedException cfe) {
        if (transientErrorRetryCount > 0 && cfe.getConflict() && cfe.getRetryable() &&
          hasSameContent(
            logStore,
            hadoopConf,
            logPath,
            CoordinatedCommitsUtils.getBackfilledDeltaFilePath(logPath, commitVersion),
            commitFile.getPath())) {
          // The commit was persisted in UC, but we did not get a response. Continue
          // because the commit was successful
          eventData.put("alreadyBackfilledCommitCausedConflict", true);
          break;
        } else {
          // Rethrow the exception here as is because the caller needs to handle it.
          recordUsageLog.accept(Optional.of(cfe), UCCoordinatedCommitsUsageLogs.UC_COMMIT_STATS);
          throw cfe;
        }
      } catch (IOException ioe) {
        if (transientErrorRetryCount == MAX_RETRIES_ON_TRANSIENT_ERROR) {
          // Rethrow exception in case we've reached the retry limit.
          recordUsageLog.accept(Optional.of(ioe), UCCoordinatedCommitsUsageLogs.UC_COMMIT_STATS);
          throw new CommitFailedException(
            true /* retryable */,
            false /* conflict */,
            ioe.getMessage(),
            ioe);
        }
        // Exponentially back off. The initial wait time is set to 100ms and the max retry count
        // is 15. The max wait time is 1 min so overall, we'll be waiting for a max of ~8 min.
        long sleepTime = Math.min(
          TRANSIENT_ERROR_RETRY_INITIAL_WAIT_MS << transientErrorRetryCount,
          TRANSIENT_ERROR_RETRY_MAX_WAIT_MS
        );
        LOG.info("Sleeping for " + sleepTime + "ms before retrying commit after transient error " +
          ioe.getMessage());
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        transientErrorRetryCount++;
        eventData.put("transientErrorRetryCount", transientErrorRetryCount);
      } catch (UpgradeNotAllowedException
          unae) {
        // This is translated to a non-retryable, non-conflicting commit failure.
        recordUsageLog.accept(Optional.of(unae), UCCoordinatedCommitsUsageLogs.UC_COMMIT_STATS);
        throw new CommitFailedException(
          false /* retryable */,
          false /* conflict */,
          unae.getMessage(),
          unae);
      } catch (InvalidTargetTableException
          itte) {
        // Just rethrow, this will propagate to the user.
        recordUsageLog.accept(Optional.of(itte), UCCoordinatedCommitsUsageLogs.UC_COMMIT_STATS);
        throw new CommitFailedException(
          false /* retryable */,
          false /* conflict */,
          itte.getMessage(),
          itte);
      } catch (CommitLimitReachedException
          clre) {
        // We attempt a full backfill and then retry the commit.
        try {
          AtomicReference<Exception> caughtException = new AtomicReference<>(null);
          lastKnownBackfilledVersion.getAndUpdate(lastKnownBackfilledVersionVal -> {
            try {
              return attemptFullBackfill(
                logStore,
                hadoopConf,
                tableDesc,
                commitVersion,
                tableId,
                lastKnownBackfilledVersionVal,
                eventData
              );
            } catch (Exception e) {
              caughtException.set(e);
              return lastKnownBackfilledVersionVal; // Return unchanged value on exception
            }
          });
          if (caughtException.get() != null) {
            throw caughtException.get();
          }
        } catch (Throwable e) {
          recordUsageLog.accept(
            Optional.of(e), UCCoordinatedCommitsUsageLogs.UC_FULL_BACKFILL_ATTEMPT_FAILED);
          String message = String.format(
            "Commit limit reached (%s) for table %s. A full backfill attempt failed due to: %s",
            exceptionString(clre),
            tableId,
            exceptionString(e));
          throw new CommitFailedException(
            true /* retryable */,
            false /* conflict */,
            message,
            clre);
        }
        eventData.put("lastKnownBackfilledVersion", lastKnownBackfilledVersion.get());
        eventData.put("encounteredCommitLimitReachedException", true);
        // Retry the commit as there should be space in UC now. We set isCommitLimitReachedRetry
        // to true so that in case the full backfill attempt was unsuccessful in freeing up space
        // in UC, we don't indefinitely retry but rather throw the CommitLimitReachedException.
        // Don't increase transientErrorRetryCount as this is not a transient error.
      } catch (UCCommitCoordinatorException
          ucce) {
        // Just rethrow, this will propagate to the user.
        recordUsageLog.accept(Optional.of(ucce), UCCoordinatedCommitsUsageLogs.UC_COMMIT_STATS);
        throw new CommitFailedException(
          false /* retryable */,
          false /* conflict */,
          ucce.getMessage(),
          ucce);
      }
    }

    LOG.info("Successfully wrote " + commitFile.getPath() + " as commit " + commitVersion +
      " to table " + tableId);

    // Asynchronously backfill everything up to the latest commit.
    Callable<Void> doBackfill = () -> {
      backfillToVersion(
        logStore,
        hadoopConf,
        tableDesc,
        commitVersion,
        lastKnownBackfilledVersion.get()
      );
      return null;
    };

    try {
      executeAsync(doBackfill);
    } catch (Throwable e) {
      if (LogStoreErrors.isFatal(e)) {
        throw e;
      }
      // attempt a synchronous backfill
      LOG.warn("Error while submitting backfill task: " + exceptionString(e) +
        ". Performing synchronous backfill now.");
      recordUsageLog.accept(
        Optional.of(e),
        UCCoordinatedCommitsUsageLogs.UC_BACKFILL_FALLBACK_TO_SYNC);
      try {
        doBackfill.call();
      } catch (Throwable t) {
        if (LogStoreErrors.isFatal(t)) {
          throw new RuntimeException(t);
        }
      }
    }

    recordUsageLog.accept(Optional.empty(), UCCoordinatedCommitsUsageLogs.UC_COMMIT_STATS);
    return new CommitResponse(new Commit(commitVersion, commitFile, commitTimestamp));
  }

  /**
   * Attempts a full backfill of all currently unbackfilled versions in order to free
   * up space in UC. After the attempt, will do a listing to find the new last known
   * backfilled version and returns it.
   */
  protected long attemptFullBackfill(
      LogStore logStore,
      Configuration hadoopConf,
      TableDescriptor tableDesc,
      long commitVersion,
      String tableId,
      long lastKnownBackfilledVersion,
      Map<String, Object> eventData) throws IOException,
        UCCommitCoordinatorException,
        CommitFailedException {
    Path logPath = tableDesc.getLogPath();
    LOG.info("Too many unbackfilled commits in UC at version {} for table at {} " +
      "and ID {}. Last known backfill version is {}. Attempting a full backfill.",
      commitVersion, logPath, tableId, lastKnownBackfilledVersion);

    long backfillStartTime = System.currentTimeMillis();
      backfillToVersion(
        logStore,
        hadoopConf,
        tableDesc,
        commitVersion,
        lastKnownBackfilledVersion
      );
    long backfillDuration = System.currentTimeMillis() - backfillStartTime;

    long updatedLastKnownBackfilledVersion = getLastKnownBackfilledVersion(
      commitVersion,
      hadoopConf,
      logStore,
      tableDesc);

    long commitStartTime = System.currentTimeMillis();
    commitToUC(
      tableDesc,
      logPath,
      Optional.empty() /* commitFile */,
      Optional.empty() /* commitVersion */,
      Optional.empty() /* commitTimestamp */,
      Optional.of(updatedLastKnownBackfilledVersion),
      CatalogTrackedInfo.EMPTY
      , true /* disown */,
      Optional.empty() /* newMetadata */,
      Optional.empty() /* newProtocol */
    );
    long commitDuration = System.currentTimeMillis() - commitStartTime;

    recordDeltaEvent(
      UCCoordinatedCommitsUsageLogs.UC_ATTEMPT_FULL_BACKFILL,
      new HashMap<String, Object>(eventData) {{
        put("commitVersion", commitVersion);
        put("coordinatedCommitsTableConf", tableDesc.getTableConf());
        put("lastKnownBackfilledVersion", lastKnownBackfilledVersion);
        put("updatedLastKnownBackfilledVersion", updatedLastKnownBackfilledVersion);
        put("tableId", tableId);
        put("backfillTime", backfillDuration);
        put("ucCommitTime", commitDuration);
      }},
      logPath.getParent()
    );
    return updatedLastKnownBackfilledVersion;
  }

  protected void commitToUC(
      TableDescriptor tableDesc,
      Path logPath,
      Optional<FileStatus> commitFile,
      Optional<Long> commitVersion,
      Optional<Long> commitTimestamp,
      Optional<Long> lastKnownBackfilledVersion,
      CatalogTrackedInfo catalogTrackedInfo,
      boolean disown,
      Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> newProtocol
  ) throws IOException, CommitFailedException, UCCommitCoordinatorException
  {
    Optional<Commit> commit = commitFile.map(f -> new Commit(
      commitVersion.orElseThrow(() -> new IllegalArgumentException(
        "Commit version should be specified when commitFile is present")),
      f,
      commitTimestamp.orElseThrow(() -> new IllegalArgumentException(
        "Commit timestamp should be specified when commitFile is present"))
    ));
    // Use Delta REST API if available and table identifier is present
    if (shouldUseDeltaRestApi(tableDesc)) {
      String[] tablePath = extractTablePathForDeltaRest(tableDesc);
      LOG.info("Using Delta REST API for commit to {}.{}.{}",
        tablePath[0], tablePath[1], tablePath[2]);
      commitViaDeltaRestApi(
        tablePath[0], tablePath[1], tablePath[2],
        commit, lastKnownBackfilledVersion, newMetadata, newProtocol
      );
    } else {
      // Fall back to old UC API
      LOG.info("Using legacy UC API for commit");
      ucClient.commit(
        extractUCTableId(tableDesc),
        CoordinatedCommitsUtils.getTablePath(logPath).toUri(),
        commit,
        lastKnownBackfilledVersion,
        disown,
        newMetadata,
        newProtocol,
        catalogTrackedInfo.deltaUniformIceberg()
      );
    }
  }

  /**
   * Commits via Delta REST Catalog API.
   */
  private void commitViaDeltaRestApi(
      String catalog,
      String schema,
      String table,
      Optional<Commit> commit,
      Optional<Long> lastKnownBackfilledVersion,
      Optional<AbstractMetadata> newMetadata,
      Optional<AbstractProtocol> newProtocol
  ) throws IOException, CommitFailedException {
    try {
      io.unitycatalog.client.deltarest.ApiClient apiClient =
          new io.unitycatalog.client.deltarest.ApiClient();
      apiClient.updateBaseUri(baseUri + "/api/2.1/unity-catalog/delta/v1");
      apiClient.setRequestInterceptor(builder -> {
        if (tokenProvider != null) {
          builder.header("Authorization", "Bearer " + tokenProvider.accessToken());
        }
      });

      io.unitycatalog.client.deltarest.api.TablesApi tablesApi =
          new io.unitycatalog.client.deltarest.api.TablesApi(apiClient);

      io.unitycatalog.client.deltarest.model.LoadTableResponse currentTable =
          tablesApi.loadTable(catalog, schema, table, false);
      io.unitycatalog.client.deltarest.model.TableMetadata currentMetadata =
          currentTable.getMetadata();

      // Build UpdateTableRequest with updates
      io.unitycatalog.client.deltarest.model.UpdateTableRequest updateRequest =
          new io.unitycatalog.client.deltarest.model.UpdateTableRequest();
      List<io.unitycatalog.client.deltarest.model.TableRequirement> requirements =
          new ArrayList<>();
      if (currentMetadata.getTableUuid() != null) {
        io.unitycatalog.client.deltarest.model.AssertTableUUID assertTableUUID =
            new io.unitycatalog.client.deltarest.model.AssertTableUUID();
        assertTableUUID.setType(
            io.unitycatalog.client.deltarest.model.AssertTableUUID.TypeEnum.ASSERT_TABLE_UUID);
        assertTableUUID.setUuid(currentMetadata.getTableUuid());
        requirements.add(new io.unitycatalog.client.deltarest.model.TableRequirement(assertTableUUID));
      }
      if (currentMetadata.getEtag() != null) {
        io.unitycatalog.client.deltarest.model.AssertEtag assertEtag =
            new io.unitycatalog.client.deltarest.model.AssertEtag();
        assertEtag.setType(io.unitycatalog.client.deltarest.model.AssertEtag.TypeEnum.ASSERT_ETAG);
        assertEtag.setEtag(currentMetadata.getEtag());
        requirements.add(new io.unitycatalog.client.deltarest.model.TableRequirement(assertEtag));
      }
      updateRequest.setRequirements(requirements);
      List<io.unitycatalog.client.deltarest.model.TableUpdate> updates = new ArrayList<>();

      if (newMetadata.isPresent()) {
        addMetadataUpdates(updates, currentMetadata, newMetadata.get());
      }
      if (newProtocol.isPresent()) {
        addProtocolUpdate(updates, currentMetadata, newProtocol.get());
      }

      // Add commit update if present
      if (commit.isPresent()) {
        Commit c = commit.get();
        io.unitycatalog.client.deltarest.model.DeltaCommit deltaCommit =
            new io.unitycatalog.client.deltarest.model.DeltaCommit();
        deltaCommit.setVersion(c.getVersion());
        deltaCommit.setTimestamp(c.getCommitTimestamp());
        deltaCommit.setFileName(c.getFileStatus().getPath().getName());
        deltaCommit.setFileSize(c.getFileStatus().getLen());
        deltaCommit.setFileModificationTimestamp(c.getFileStatus().getModificationTime());

        io.unitycatalog.client.deltarest.model.AddCommitUpdate addCommit =
            new io.unitycatalog.client.deltarest.model.AddCommitUpdate();
        addCommit.setAction(
            io.unitycatalog.client.deltarest.model.AddCommitUpdate.ActionEnum.ADD_COMMIT);
        addCommit.setCommit(deltaCommit);
        updates.add(new io.unitycatalog.client.deltarest.model.TableUpdate(addCommit));
      }

      // Add backfill version update if present
      if (lastKnownBackfilledVersion.isPresent()) {
        io.unitycatalog.client.deltarest.model.SetLatestBackfilledVersionUpdate backfillUpdate =
            new io.unitycatalog.client.deltarest.model.SetLatestBackfilledVersionUpdate();
        backfillUpdate.setAction(
            io.unitycatalog.client.deltarest.model.SetLatestBackfilledVersionUpdate
                .ActionEnum.SET_LATEST_BACKFILLED_VERSION);
        backfillUpdate.setLatestPublishedVersion(lastKnownBackfilledVersion.get());
        updates.add(new io.unitycatalog.client.deltarest.model.TableUpdate(backfillUpdate));
      }

      updateRequest.setUpdates(updates);
      tablesApi.updateTable(catalog, schema, table, updateRequest);

    } catch (io.unitycatalog.client.deltarest.ApiException e) {
      if (e.getCode() == 409) {
        throw new CommitFailedException(
          true /* retryable */,
          true /* conflict */,
          "Commit conflict: " + e.getResponseBody(),
          e);
      } else if (e.getCode() >= 400 && e.getCode() < 500) {
        throw new CommitFailedException(
          false /* retryable */,
          false /* conflict */,
          "Bad request: " + e.getResponseBody(),
          e);
      } else {
        throw new CommitFailedException(
          true /* retryable */,
          false /* conflict */,
          "Commit failed with HTTP " + e.getCode() + ": " + e.getResponseBody(),
          e);
      }
    }
  }

  private void addMetadataUpdates(
      List<io.unitycatalog.client.deltarest.model.TableUpdate> updates,
      io.unitycatalog.client.deltarest.model.TableMetadata currentMetadata,
      AbstractMetadata newMetadata) throws IOException {
    if (!Objects.equals(currentMetadata.getComment(), newMetadata.getDescription()) &&
        newMetadata.getDescription() != null) {
      io.unitycatalog.client.deltarest.model.SetTableCommentUpdate commentUpdate =
          new io.unitycatalog.client.deltarest.model.SetTableCommentUpdate();
      commentUpdate.setAction(
          io.unitycatalog.client.deltarest.model.SetTableCommentUpdate.ActionEnum.SET_TABLE_COMMENT);
      commentUpdate.setComment(newMetadata.getDescription());
      updates.add(new io.unitycatalog.client.deltarest.model.TableUpdate(commentUpdate));
    }

    List<String> newPartitionColumns =
        newMetadata.getPartitionColumns() == null ?
            Collections.emptyList() :
            new ArrayList<>(newMetadata.getPartitionColumns());
    List<String> currentPartitionColumns =
        currentMetadata.getPartitionColumns() == null ?
            Collections.emptyList() :
            currentMetadata.getPartitionColumns();
    if (!currentPartitionColumns.equals(newPartitionColumns)) {
      io.unitycatalog.client.deltarest.model.SetPartitionColumnsUpdate partitionUpdate =
          new io.unitycatalog.client.deltarest.model.SetPartitionColumnsUpdate();
      partitionUpdate.setAction(
          io.unitycatalog.client.deltarest.model.SetPartitionColumnsUpdate.ActionEnum
              .SET_PARTITION_COLUMNS);
      partitionUpdate.setPartitionColumns(newPartitionColumns);
      updates.add(new io.unitycatalog.client.deltarest.model.TableUpdate(partitionUpdate));
    }

    List<io.unitycatalog.client.deltarest.model.DeltaColumn> newColumns =
        toDeltaColumns(newMetadata.getSchemaString());
    List<io.unitycatalog.client.deltarest.model.DeltaColumn> currentColumns =
        currentMetadata.getColumns() == null ?
            Collections.emptyList() :
            currentMetadata.getColumns();
    if (!currentColumns.equals(newColumns)) {
      io.unitycatalog.client.deltarest.model.SetColumnsUpdate columnsUpdate =
          new io.unitycatalog.client.deltarest.model.SetColumnsUpdate();
      columnsUpdate.setAction(
          io.unitycatalog.client.deltarest.model.SetColumnsUpdate.ActionEnum.SET_COLUMNS);
      columnsUpdate.setColumns(newColumns);
      updates.add(new io.unitycatalog.client.deltarest.model.TableUpdate(columnsUpdate));
    }

    Map<String, String> currentProperties =
        currentMetadata.getProperties() == null ?
            Collections.emptyMap() :
            currentMetadata.getProperties();
    Map<String, String> desiredProperties =
        newMetadata.getConfiguration() == null ?
            Collections.emptyMap() :
            newMetadata.getConfiguration();
    Map<String, String> changedProperties = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : desiredProperties.entrySet()) {
      if (shouldSkipCatalogPropertyUpdate(entry.getKey())) {
        continue;
      }
      if (!Objects.equals(currentProperties.get(entry.getKey()), entry.getValue())) {
        changedProperties.put(entry.getKey(), entry.getValue());
      }
    }
    if (!changedProperties.isEmpty()) {
      io.unitycatalog.client.deltarest.model.SetPropertiesUpdate propertiesUpdate =
          new io.unitycatalog.client.deltarest.model.SetPropertiesUpdate();
      propertiesUpdate.setAction(
          io.unitycatalog.client.deltarest.model.SetPropertiesUpdate.ActionEnum.SET_PROPERTIES);
      propertiesUpdate.setUpdates(changedProperties);
      updates.add(new io.unitycatalog.client.deltarest.model.TableUpdate(propertiesUpdate));
    }
  }

  private boolean shouldSkipCatalogPropertyUpdate(String propertyKey) {
    return PROP_IS_MANAGED_LOCATION.equals(propertyKey);
  }

  private void addProtocolUpdate(
      List<io.unitycatalog.client.deltarest.model.TableUpdate> updates,
      io.unitycatalog.client.deltarest.model.TableMetadata currentMetadata,
      AbstractProtocol newProtocol) {
    io.unitycatalog.client.deltarest.model.DeltaProtocol desiredProtocol =
        toDeltaProtocol(newProtocol);
    if (!desiredProtocol.equals(currentMetadata.getProtocol())) {
      io.unitycatalog.client.deltarest.model.SetProtocolUpdate protocolUpdate =
          new io.unitycatalog.client.deltarest.model.SetProtocolUpdate();
      protocolUpdate.setAction(
          io.unitycatalog.client.deltarest.model.SetProtocolUpdate.ActionEnum.SET_PROTOCOL);
      protocolUpdate.setProtocol(desiredProtocol);
      updates.add(new io.unitycatalog.client.deltarest.model.TableUpdate(protocolUpdate));
    }
  }

  private io.unitycatalog.client.deltarest.model.DeltaProtocol toDeltaProtocol(
      AbstractProtocol protocol) {
    io.unitycatalog.client.deltarest.model.DeltaProtocol deltaProtocol =
        new io.unitycatalog.client.deltarest.model.DeltaProtocol();
    deltaProtocol.setMinReaderVersion(protocol.getMinReaderVersion());
    deltaProtocol.setMinWriterVersion(protocol.getMinWriterVersion());
    if (protocol.getReaderFeatures() != null && !protocol.getReaderFeatures().isEmpty()) {
      List<String> readerFeatures = new ArrayList<>(protocol.getReaderFeatures());
      Collections.sort(readerFeatures);
      deltaProtocol.setReaderFeatures(readerFeatures);
    }
    if (protocol.getWriterFeatures() != null && !protocol.getWriterFeatures().isEmpty()) {
      List<String> writerFeatures = new ArrayList<>(protocol.getWriterFeatures());
      Collections.sort(writerFeatures);
      deltaProtocol.setWriterFeatures(writerFeatures);
    }
    return deltaProtocol;
  }

  private List<io.unitycatalog.client.deltarest.model.DeltaColumn> toDeltaColumns(
      String schemaString) throws IOException {
    if (schemaString == null || schemaString.isEmpty()) {
      return Collections.emptyList();
    }
    JsonNode schemaNode = JSON_MAPPER.readTree(schemaString);
    JsonNode fieldsNode = schemaNode.get("fields");
    if (fieldsNode == null || !fieldsNode.isArray()) {
      throw new IOException("Invalid Delta schema JSON: missing top-level fields array");
    }

    List<io.unitycatalog.client.deltarest.model.DeltaColumn> columns = new ArrayList<>();
    for (JsonNode fieldNode : fieldsNode) {
      io.unitycatalog.client.deltarest.model.DeltaColumn column =
          new io.unitycatalog.client.deltarest.model.DeltaColumn();
      column.setName(fieldNode.get("name").asText());
      JsonNode typeNode = fieldNode.get("type");
      if (typeNode != null && !typeNode.isNull()) {
        column.setType(JSON_MAPPER.convertValue(typeNode, Object.class));
      }
      JsonNode nullableNode = fieldNode.get("nullable");
      column.setNullable(nullableNode == null || nullableNode.asBoolean());
      JsonNode metadataNode = fieldNode.get("metadata");
      Map<String, Object> metadata =
          metadataNode == null || metadataNode.isNull() ?
              new LinkedHashMap<>() :
              JSON_MAPPER.convertValue(metadataNode, LinkedHashMap.class);
      column.setMetadata(metadata);
      columns.add(column);
    }
    return columns;
  }

  /**
   * Detects whether the current commit is a downgrade (disown) commit by checking
   * that the UC commit coordinator name is present in the old metadata but removed from
   * the new metadata.
   */
  protected boolean isDisownCommit(AbstractMetadata oldMetadata, AbstractMetadata newMetadata) {
    return CoordinatedCommitsUtils
      .getCoordinatorName(oldMetadata)
      .filter("unity-catalog"::equals).isPresent() &&
      !CoordinatedCommitsUtils.getCoordinatorName(newMetadata).isPresent();
  }

  /**
   * This method provides idempotency under network failures by verifying whether the currently
   * attempted commit already exists as a backfilled commit. This prevents duplicate data from
   * being written when UC returns a retryable conflict for a commit that was actually successful
   * but the client didn't receive the success response.
   *
   * Failure sequence requiring this check:
   * 1. Client attempts to make commit v.
   * 2. UC persists the commit in its database but the connection to the client breaks.
   * 3. The client receives a transient error (retryable=true, conflict=false).
   * 4. Before retrying, a concurrent client commits v + 1 and backfills v.
   * 5. Another subsequent commit registers the backfill of v with UC, leading UC to
   *    delete the commit for v from its database.
   * 6. Now this client retries commit v (without conflict resolution since conflict=false
   *    in step 3).
   * 7. UC rejects the commit because v {@literal <=} latest_table_version and returns a retryable
   *    conflict (retryable=true, conflict=true).
   *
   * Without this check, Delta's default response to retryable=true, conflict=true would be to
   * rebase the commit on top of the latest table version and retry, effectively trying to
   * commit the contents of v as v+2. This would result in duplicate data being written.
   *
   * This method prevents that by checking if the backfilled commit (v.json) has the same
   * content as our retry attempt (v.{@literal <uuid>}.json). If yes, we know our original commit
   * succeeded and can safely ignore the conflict and exit early without rebasing.
   *
   * Below is a concrete example of the failure and retry sequence:
   * - Attempt 1: Try to commit v. UC responds with retryable=true, conflict=false under
   *              network failure.
   * - Attempt 2: Try to commit v without conflict resolution since conflict=false in attempt-1.
   *              UC responds with retryable=true, conflict=true in the above scenario.
   *              (i.e. v is backfilled and latest version is v+1).
   * - Fix: Compare v.{@literal <uuid>}.json and v.json and *early exit* here.
   * - Attempt 3: [Without fix] Rebase, conflict-resolution + Try to commit v+2
   *              {@literal =>} double-commit for contents of v {@literal =>} bug.
   */
  protected boolean hasSameContent(
      LogStore logStore,
      Configuration hadoopConf,
      Path logPath,
      Path backfilledCommit,
      Path unbackfilledCommit) {
    try {
      FileSystem fs = logPath.getFileSystem(hadoopConf);
      if (fs.getFileStatus(backfilledCommit).getLen() !=
          fs.getFileStatus(unbackfilledCommit).getLen()) {
        return false;
      }
    } catch (FileNotFoundException e) {
      // If we get a FileNotFoundException, it should be for the backfilled
      // commit because we are only calling this method from commit() at the moment,
      // which means we just wrote the unbackfilled commit.
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Compare content.
    try (CloseableIterator<String> contentBackfilled = logStore.read(backfilledCommit, hadoopConf);
         CloseableIterator<String> contentUnbackfilled =
           logStore.read(unbackfilledCommit, hadoopConf)) {
      while (contentUnbackfilled.hasNext() && contentBackfilled.hasNext()) {
        if (!contentUnbackfilled.next().equals(contentBackfilled.next())) {
          return false;
        }
      }
      return !contentBackfilled.hasNext() && !contentUnbackfilled.hasNext();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public GetCommitsResponse getCommits(
      TableDescriptor tableDesc,
      Long startVersion,
      Long endVersion) {
    checkVersionSupported(tableDesc.getTableConf(), true /* compareRead */);
    GetCommitsResponse resp = getCommitsFromUCImpl(
      tableDesc,
      Optional.ofNullable(startVersion),
      Optional.ofNullable(endVersion));
    // Sort by version just in case commits in the response from UC aren't sorted.
    List<Commit> sortedCommits =
      resp
        .getCommits()
        .stream()
        .sorted(Comparator.comparingLong(Commit::getVersion))
        .collect(Collectors.toList());
    return new GetCommitsResponse(sortedCommits, resp.getLatestTableVersion());
  }

  protected GetCommitsResponse getCommitsFromUCImpl(
      TableDescriptor tableDesc,
      Optional<Long> startVersion,
      Optional<Long> endVersion) {
    try {
      // Use Delta REST API if available
      if (shouldUseDeltaRestApi(tableDesc)) {
        String[] tablePath = extractTablePathForDeltaRest(tableDesc);
        LOG.info("Using Delta REST API for getCommits from {}.{}.{}",
          tablePath[0], tablePath[1], tablePath[2]);
        return getCommitsViaDeltaRestApi(
          tablePath[0], tablePath[1], tablePath[2],
          startVersion, endVersion
        );
      } else {
        LOG.info("Using legacy UC API for getCommits");
        return ucClient.getCommits(
          extractUCTableId(tableDesc),
          CoordinatedCommitsUtils.getTablePath(tableDesc.getLogPath()).toUri(),
          startVersion,
          endVersion);
      }
    } catch (IOException | UCCommitCoordinatorException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets commits via Delta REST Catalog API.
   */
  private GetCommitsResponse getCommitsViaDeltaRestApi(
      String catalog,
      String schema,
      String table,
      Optional<Long> startVersion,
      Optional<Long> endVersion
  ) throws IOException {
    try {
      io.unitycatalog.client.deltarest.ApiClient apiClient =
          new io.unitycatalog.client.deltarest.ApiClient();
      apiClient.updateBaseUri(baseUri + "/api/2.1/unity-catalog/delta/v1");
      apiClient.setRequestInterceptor(builder -> {
        if (tokenProvider != null) {
          builder.header("Authorization", "Bearer " + tokenProvider.accessToken());
        }
      });

      io.unitycatalog.client.deltarest.api.TablesApi tablesApi =
          new io.unitycatalog.client.deltarest.api.TablesApi(apiClient);

      // Load table to get commits
      io.unitycatalog.client.deltarest.model.LoadTableResponse response =
          tablesApi.loadTable(catalog, schema, table, false);

      // Extract commits from response
      List<io.unitycatalog.client.deltarest.model.DeltaCommit> deltaCommits =
          response.getCommits();
      if (deltaCommits == null) {
        deltaCommits = new ArrayList<>();
      }

      // Filter by version range
      long start = startVersion.orElse(0L);
      long end = endVersion.orElse(Long.MAX_VALUE);

      // We need the table path to construct Commit objects
      String tableLoc = response.getMetadata().getLocation();
      Path tablePathObj = new Path(tableLoc);
      Path commitDir = CoordinatedCommitsUtils.commitDirPath(
        CoordinatedCommitsUtils.logDirPath(tablePathObj));

      List<Commit> commits = deltaCommits.stream()
        .filter(dc -> dc.getVersion() >= start && dc.getVersion() <= end)
        .map(dc -> {
          Path commitFile = new Path(commitDir, dc.getFileName());
          FileStatus fileStatus = new FileStatus(
            dc.getFileSize(),
            false /* isdir */,
            0 /* block_replication */,
            0 /* blocksize */,
            dc.getFileModificationTimestamp(),
            commitFile);
          return new Commit(dc.getVersion(), fileStatus, dc.getTimestamp());
        })
        .collect(Collectors.toList());

      Long latestTableVersion = response.getLatestTableVersion();
      return new GetCommitsResponse(
          commits, latestTableVersion != null ? latestTableVersion : -1L);

    } catch (io.unitycatalog.client.deltarest.ApiException e) {
      throw new IOException("Failed to get commits: " + e.getResponseBody(), e);
    }
  }

  @Override
  public void backfillToVersion(
      LogStore logStore,
      Configuration hadoopConf,
      TableDescriptor tableDesc,
      long version,
      Long lastKnownBackfilledVersion) throws IOException {
    // backfillToVersion currently does not depend on write. However, it is
    // technically a write operation, so we also add a write version check here
    // in case we ever introduce a write dependency.
    checkVersionSupported(tableDesc.getTableConf(), true /* compareRead */);
    checkVersionSupported(tableDesc.getTableConf(), false /* compareRead */);

    Path logPath = tableDesc.getLogPath();
    String tableId = extractUCTableId(tableDesc);
    long startVersion = (lastKnownBackfilledVersion == null) ? 0L : lastKnownBackfilledVersion;
    long startTimeMs = System.currentTimeMillis();
    LOG.info("Backfilling {}: startVersion {} to endVersion {}", tableId, startVersion, version);

    // Check that the last known backfilled version actually exists if it
    // has been specified. If it doesn't exist, we fail the backfill. If it
    // hasn't been specified backfill everything that hasn't been backfilled yet.
    if (lastKnownBackfilledVersion != null) {
      FileSystem fs = logPath.getFileSystem(hadoopConf);
      // Check that the last known backfilled version actually exists.
      if (!fs.exists(CoordinatedCommitsUtils
          .getBackfilledDeltaFilePath(logPath, lastKnownBackfilledVersion))) {
        LOG.error("Specified last known backfilled version {} does not exist for table {}",
            lastKnownBackfilledVersion, tableId);
        recordDeltaEvent(
          UCCoordinatedCommitsUsageLogs.UC_BACKFILL_DOES_NOT_EXIST,
          new HashMap<String, Object>() {{
            put("lastKnownBackfilledVersion", lastKnownBackfilledVersion);
            put("version", version);
            put("tableConf", tableDesc.getTableConf());
          }},
          logPath.getParent()
        );
        throw new IllegalStateException("Last known backfilled version " +
          lastKnownBackfilledVersion + " doesn't exist for table at " + logPath);
      }
    }
    GetCommitsResponse commitsResponse = getCommits(tableDesc, lastKnownBackfilledVersion, version);
    for (Commit commit : commitsResponse.getCommits()) {
      boolean backfillResult = backfillSingleCommit(
        logStore,
        hadoopConf,
        logPath,
        commit.getVersion(),
        commit.getFileStatus(),
        false /* failOnException */);
      if (!backfillResult) {
        break;
      }
    }

    recordDeltaEvent(
      UCCoordinatedCommitsUsageLogs.UC_BACKFILL_TO_VERSION,
      new HashMap<String, Object>() {{
        put("coordinatedCommitsTableConf", tableDesc.getTableConf());
        put("totalTimeTakenMs", System.currentTimeMillis() - startTimeMs);
        put("lastKnownBackfilledVersion", lastKnownBackfilledVersion);
        put("tableId", tableId);
        put("version", version);
      }},
      logPath.getParent()
    );
  }

  /**
   * Backfill the specified commit as the target version. Returns true if the
   * backfill was successful (or the backfilled file already existed) and false
   * in case the backfill failed.
   */
  protected boolean backfillSingleCommit(
      LogStore logStore,
      Configuration hadoopConf,
      Path logPath,
      long version,
      FileStatus fileStatus,
      Boolean failOnException) {
    Path targetFile = CoordinatedCommitsUtils.getBackfilledDeltaFilePath(logPath, version);
    try (CloseableIterator<String> commitContentIterator =
           logStore.read(fileStatus.getPath(), hadoopConf)) {
      // Use put-if-absent for backfills so that files are not overwritten and the
      // modification time does not change for already backfilled files.
      logStore.write(targetFile, commitContentIterator, false /* overwrite */, hadoopConf);
    } catch (FileAlreadyExistsException e) {
      LOG.info("The backfilled file {} already exists.", targetFile);
    } catch (Exception e) {
      if (LogStoreErrors.isFatal(e) || failOnException) {
        throw new RuntimeException(e);
      }
      LOG.warn("Backfill for table at {} failed for version {} due to: {}",
        logPath, version, exceptionString(e));
      recordDeltaEvent(
        UCCoordinatedCommitsUsageLogs.UC_BACKFILL_FAILED,
        new HashMap<String, Object>() {{
          put("version", version);
          put("exceptionClass", e.getClass().getName());
          put("exceptionString", exceptionString(e));
        }},
        logPath.getParent()
      );
      return false;
    }
    return true;
  }

  @Override
  public boolean semanticEquals(CommitCoordinatorClient other) {
    if (!(other instanceof UCCommitCoordinatorClient)) {
      return false;
    }
    UCCommitCoordinatorClient otherStore = (UCCommitCoordinatorClient) other;
    return this.conf == otherStore.conf;
  }

  protected void recordDeltaEvent(String opType, Object data, Path path) {
    LOG.info("Delta event recorded with opType={}, data={}, and path={}", opType, data, path);
  }

  protected String exceptionString(Throwable e) {
    if (e == null) {
      return "";
    } else {
      StringWriter stringWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stringWriter));
      return stringWriter.toString();
    }
  }

  protected void checkVersionSupported(Map<String, String> tableConf, boolean compareRead) {
    int readVersion = Integer.parseInt(tableConf.getOrDefault(READ_VERSION_KEY, "0"));
    int writeVersion = Integer.parseInt(tableConf.getOrDefault(WRITE_VERSION_KEY, "0"));
    int targetVersion = compareRead ? readVersion : writeVersion;
    int supportedVersion = compareRead ? SUPPORTED_READ_VERSION : SUPPORTED_WRITE_VERSION;
    String op = compareRead ? "read" : "write";
    if (supportedVersion != targetVersion) {
      throw new UnsupportedOperationException("The version of the UC commit coordinator protocol" +
      " is not supported by this version of the UC commit coordinator client. Please upgrade" +
      " the commit coordinator client to " + op + " this table.");
    }
  }

  /**
   * Extracts catalog, schema, and table name from TableDescriptor for Delta REST API calls.
   */
  private String[] extractTablePathForDeltaRest(TableDescriptor tableDesc) {
    if (!tableDesc.getTableIdentifier().isPresent()) {
      return null;
    }
    io.delta.storage.commit.TableIdentifier tableId = tableDesc.getTableIdentifier().get();
    String[] namespace = tableId.getNamespace();
    if (namespace.length != 2) {
      LOG.warn("TableIdentifier namespace has {} elements, expected 2 for Delta REST API: {}",
        namespace.length, java.util.Arrays.toString(namespace));
      return null;
    }
    return new String[]{namespace[0], namespace[1], tableId.getName()};
  }

  /**
   * Check if we should use Delta REST API based on available information.
   */
  private boolean shouldUseDeltaRestApi(TableDescriptor tableDesc) {
    return baseUri != null && tokenProvider != null &&
           extractTablePathForDeltaRest(tableDesc) != null;
  }
}
