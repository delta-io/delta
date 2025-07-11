package io.delta.dsv2.read;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.PartitionReader;

/** Created on Executor */
abstract class DeltaPartitionReader<T> implements PartitionReader<T> {
  protected final DefaultEngine engine;
  protected final Row scanFileRow;
  protected final Row scanStateRow;
  protected final FileStatus addFileStatus;
  protected final CloseableIterator<FilteredColumnarBatch> logicalRowDataColumnarBatchIter;

  protected DeltaPartitionReader(DeltaInputPartition deltaInputPartition) throws IOException {
    Configuration conf = new Configuration();
    // Set up S3 configuration
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    // Use credentials from input partition
    conf.set("fs.s3a.access.key", deltaInputPartition.getAccessKey());
    conf.set("fs.s3a.secret.key", deltaInputPartition.getSecretKey());
    conf.set("fs.s3a.session.token", deltaInputPartition.getSessionToken());
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3.impl.disable.cache", "true");
    conf.set("fs.s3a.impl.disable.cache", "true");
    conf.set("fs.s3.region", "us-west-2");
    conf.set("fs.s3.endpoint", "s3.us-west-2.amazonaws.com");

    this.engine = DefaultEngine.create(conf);

    this.scanFileRow =
        JsonUtils.rowFromJson(
            deltaInputPartition.getSerializedScanFileRow(), InternalScanFileUtils.SCAN_FILE_SCHEMA);

    this.addFileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);

    this.scanStateRow =
        JsonUtils.rowFromJson(
            deltaInputPartition.getSerializedScanState(), ScanStateRowHelper.getSchema());

    CloseableIterator<ColumnarBatch> physicalRowDataIter =
        engine
            .getParquetHandler()
            .readParquetFiles(
                Utils.singletonCloseableIterator(addFileStatus),
                ScanStateRow.getPhysicalDataReadSchema(engine, scanStateRow),
                java.util.Optional.empty() /* predicate */)
            .map(FileReadResult::getData);

    this.logicalRowDataColumnarBatchIter =
        Scan.transformPhysicalData(engine, scanStateRow, scanFileRow, physicalRowDataIter);
  }
}
