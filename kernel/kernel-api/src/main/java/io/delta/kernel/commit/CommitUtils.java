package io.delta.kernel.commit;

import io.delta.kernel.internal.util.FileNames;

public class CommitUtils {

  private CommitUtils() { }

  public static String getStagedCommitFilePath(String logPath, long commitVersion) {
    return FileNames.stagedCommitFile(logPath, commitVersion);
  }
}
