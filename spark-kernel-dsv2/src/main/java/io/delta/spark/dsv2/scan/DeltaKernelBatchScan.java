package io.delta.spark.dsv2.scan;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class DeltaKernelBatchScan implements Batch {
    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[0];
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return null;
    }
}
