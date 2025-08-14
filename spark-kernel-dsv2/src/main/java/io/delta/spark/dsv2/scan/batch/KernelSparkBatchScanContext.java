package io.delta.spark.dsv2.scan.batch;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.dsv2.utils.ResettableIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.connector.read.InputPartition;

public class KernelSparkBatchScanContext {

    private final ResettableIterator<FilteredColumnarBatch> postStaticPruningScanFiles;
    private final String serializedScanState;
    private InputPartition[] cachedPartitions;

    public KernelSparkBatchScanContext(
            ResettableIterator<FilteredColumnarBatch> postStaticPruningScanFiles,
            Row scanStateRow) {
        this.postStaticPruningScanFiles = postStaticPruningScanFiles;
        this.serializedScanState = JsonUtils.rowToJson(scanStateRow);
    }

    public synchronized InputPartition[] planPartitionsOnce() {
        if (cachedPartitions != null) {
            return cachedPartitions;
        }

        postStaticPruningScanFiles.reset();
        List<InputPartition> partitions = new ArrayList<>();

        try {
            while (postStaticPruningScanFiles.hasNext()) {
                FilteredColumnarBatch batch = postStaticPruningScanFiles.next();
                CloseableIterator<Row> rows = batch.getRows();
                try {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        partitions.add(
                                new KernelSparkInputPartition(
                                        serializedScanState, JsonUtils.rowToJson(row)));
                    }
                } finally {
                    try { rows.close(); } catch (IOException ignore) {}
                }
            }
        } finally {
            try { postStaticPruningScanFiles.close(); } catch (IOException ignore) {}
        }

        cachedPartitions = partitions.toArray(new InputPartition[0]);
        return cachedPartitions;
    }
}
