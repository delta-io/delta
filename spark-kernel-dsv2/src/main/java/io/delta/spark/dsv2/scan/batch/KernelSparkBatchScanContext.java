package io.delta.spark.dsv2.scan.batch;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.spark.dsv2.utils.ResettableIterator;

public class KernelSparkBatchScanContext {

    private final ResettableIterator<FilteredColumnarBatch> postStaticPruningScanFiles;

    public KernelSparkBatchScanContext(ResettableIterator<FilteredColumnarBatch> postStaticPruningScanFiles){
        this.postStaticPruningScanFiles = postStaticPruningScanFiles;
    }


}
