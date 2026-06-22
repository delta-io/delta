/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.table.postcommit;

import io.delta.flink.table.*;
import io.delta.flink.table.postcommit.po.CommitReport;
import io.delta.flink.table.postcommit.po.DeltaMetricsApi;
import io.delta.flink.table.postcommit.po.ReportDeltaMetricsRequest;
import io.delta.kernel.Snapshot;
import io.delta.kernel.metrics.FileSizeHistogramResult;
import io.delta.kernel.metrics.TransactionMetricsResult;
import io.delta.kernel.metrics.TransactionReport;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Post-commit listener that reports commit metrics to Unity Catalog via {@link DeltaMetricsApi}.
 *
 * <p>This listener is best-effort: failures are logged as warnings but never propagated to the
 * caller, so a metrics delivery failure cannot break the commit path.
 *
 * <p>The listener is only active when the table's catalog is a {@link UnityCatalog} and the table
 * has a resolved UUID. For all other catalog types the hook is a no-op.
 */
public class ReportCommitMetricsListener implements TableEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(ReportCommitMetricsListener.class);

  private transient DeltaMetricsApi metricsApi;

  @Override
  public void onPostCommit(
      AbstractKernelTable source, Snapshot snapshot, TransactionReport report) {
    if (!(source instanceof CatalogManagedTable)) {
      return;
    }
    DeltaCatalog catalog = source.getCatalog();
    String tableUUID = source.getTableUUID();
    if (tableUUID == null || tableUUID.isEmpty()) {
      LOG.debug("Skipping commit metrics: no table UUID for {}", source.getId());
      return;
    }

    try {
      DeltaMetricsApi api = getOrCreateApi((UnityCatalog) catalog);
      ReportDeltaMetricsRequest request = buildRequest(tableUUID, report);

      source.executeWithTiming("postcommit.reportmetrics", () -> api.reportMetrics(request));

      LOG.info(
          "Reported commit metrics for table {} version {}", source.getId(), snapshot.getVersion());
    } catch (Exception e) {
      LOG.warn(
          "Failed to report commit metrics for table {}: {}", source.getId(), e.getMessage(), e);
    }
  }

  private DeltaMetricsApi getOrCreateApi(UnityCatalog catalog) {
    if (metricsApi == null) {
      metricsApi = new DeltaMetricsApi(catalog.getApiClient());
    }
    return metricsApi;
  }

  private ReportDeltaMetricsRequest buildRequest(String tableUUID, TransactionReport txnReport) {
    TransactionMetricsResult metrics = txnReport.getTransactionMetrics();

    CommitReport report =
        new CommitReport()
            .numFilesAdded(metrics.getNumAddFiles())
            .numFilesRemoved(metrics.getNumRemoveFiles())
            .numBytesAdded(metrics.getTotalAddFilesSizeInBytes())
            .numBytesRemoved(metrics.getTotalRemoveFilesSizeInBytes());

    metrics
        .getTableFileSizeHistogram()
        .ifPresent(
            histogram ->
                report.fileSizeHistogram(
                    toFileSizeHistogram(histogram, txnReport.getCommittedVersion().orElse(null))));

    return ReportDeltaMetricsRequest.of(tableUUID, report);
  }

  private static CommitReport.FileSizeHistogram toFileSizeHistogram(
      FileSizeHistogramResult result, Long commitVersion) {
    return new CommitReport.FileSizeHistogram()
        .sortedBinBoundaries(toLongList(result.getSortedBinBoundaries()))
        .fileCounts(toLongList(result.getFileCounts()))
        .totalBytes(toLongList(result.getTotalBytes()))
        .commitVersion(commitVersion);
  }

  private static List<Long> toLongList(long[] array) {
    return Arrays.stream(array).boxed().collect(Collectors.toList());
  }
}
