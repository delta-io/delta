package io.delta.storage.catalogmanaged.unitycatalog;

import io.delta.kernel.ResolvedTable;
import io.delta.kernel.TableManager;
import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.commit.Commit;
import io.delta.storage.commit.GetCommitsResponse;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorException;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;

/**
 * Client for interacting with Unity Catalog (UC) catalog-managed Delta tables.
 *
 * @see UCClient
 * @see ResolvedTable
 */
@Experimental
public class UCCatalogManagedClient {
  private final UCClient ucClient;
  private final String ucTableId;
  private final String tablePath;

  public UCCatalogManagedClient(UCClient ucClient, String ucTableId, String tablePath) {
    this.ucClient = Objects.requireNonNull(ucClient, "ucClient is null");
    this.ucTableId = Objects.requireNonNull(ucTableId, "ucTableId is null");
    this.tablePath = Objects.requireNonNull(tablePath, "tablePath is null");
  }

  /** Loads a Kernel {@link ResolvedTable} at a specific version. */
  public ResolvedTable loadTable(Engine engine, long tableVersionToLoad) {
    final GetCommitsResponse response = getRatifiedCommitsFromUC(tableVersionToLoad);

    validateLoadTableVersionInRange(tableVersionToLoad, response.getLatestTableVersion());

    final List<ParsedLogData> logData =
        getSortedKernelLogDataFromRatifiedCommits(response.getCommits());

    return TableManager
        .loadTable(tablePath)
        .atVersion(tableVersionToLoad)
        .withLogData(logData)
        .build(engine);
  }

  private GetCommitsResponse getRatifiedCommitsFromUC(long tableVersion) {
    try {
      return ucClient.getCommits(
          ucTableId,
          new Path(tablePath).toUri(),
          Optional.empty() /* startVersion */,
          Optional.of(tableVersion) /* endVersion */);
    } catch (IOException | UCCommitCoordinatorException e) {
      throw new RuntimeException(e);
    }
  }

  private void validateLoadTableVersionInRange(long tableVersionToLoad, long maxRatifiedVersion) {
    if (tableVersionToLoad > maxRatifiedVersion) {
      throw new RuntimeException(
          String.format(
              "[%s] Cannot load table version %s as the latest ratified version is %s",
              ucTableId, tableVersionToLoad, maxRatifiedVersion
          ));
    }
  }

  private List<ParsedLogData> getSortedKernelLogDataFromRatifiedCommits(List<Commit> commits) {
    return commits
        .stream()
        .sorted(Comparator.comparingLong(Commit::getVersion))
        .map(commit -> {
      final org.apache.hadoop.fs.FileStatus hadoopFS = commit.getFileStatus();
      final io.delta.kernel.utils.FileStatus kernelFS = FileStatus.of(
          hadoopFS.getPath().toString(),
          hadoopFS.getLen(),
          hadoopFS.getModificationTime());
      return ParsedLogData.forFileStatus(kernelFS);
    }).collect(Collectors.toList());
  }
}
