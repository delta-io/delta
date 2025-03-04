package io.delta.storage.commit.uccommitcoordinator;

/** Class containing usage logs emitted by Coordinated Commits. */
public class UCCoordinatedCommitsUsageLogs {

  // Common prefix for all coordinated-commits usage logs.
  private static final String PREFIX = "delta.coordinatedCommits";

  // Usage log emitted after backfilling to a version.
  public static final String UC_BACKFILL_TO_VERSION = PREFIX + ".uc.backfillToVersion";

  // Usage log emitted if the specified last known backfilled version does not exist.
  public static final String UC_BACKFILL_DOES_NOT_EXIST = PREFIX + ".uc.backfillDoesNotExist";

  // Usage log emitted if a backfill attempt for a single file failed.
  public static final String UC_BACKFILL_FAILED = PREFIX + ".uc.backfillFailed";

  // Usage log emitted when the last known backfilled version cannot be determined from the last
  // `BACKFILL_LISTING_OFFSET` commits.
  public static final String UC_LAST_KNOWN_BACKFILLED_VERSION_NOT_FOUND =
    PREFIX + ".uc.lastKnownBackfilledVersionNotFound";

  // Usage log emitted if UC commit coordinator client falls back to synchronous backfill.
  public static final String UC_BACKFILL_VALIDATION_FALLBACK_TO_SYNC =
    PREFIX + ".uc.backfillValidation.fallbackToSync";

  // Usage log emitted when commit limit is reached, and we attempt a full backfill.
  public static final String UC_ATTEMPT_FULL_BACKFILL = PREFIX + ".uc.attemptFullBackfill";

  // Usage log emitted if UC commit coordinator client falls back to synchronous backfill.
  public static final String UC_BACKFILL_FALLBACK_TO_SYNC = PREFIX + ".uc.backfill.fallbackToSync";

  // Usage log emitted as part of [[UCCommitCoordinatorClient.commit]] call.
  public static final String UC_COMMIT_STATS = PREFIX + ".uc.commitStats";

  // Usage log emitted when a full backfill attempt has failed
  public static final String UC_FULL_BACKFILL_ATTEMPT_FAILED =
    PREFIX + ".uc.fullBackfillAttemptFailed";
}
