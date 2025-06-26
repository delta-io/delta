package io.delta.dsv2.read;

import java.io.IOException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Serialized and sent from the Driver to the Executors */
public class DeltaReaderFactory implements PartitionReaderFactory {
  private static final Logger logger = LoggerFactory.getLogger(DeltaReaderFactory.class);

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    logger.info("createReader");

    if (!(partition instanceof DeltaInputPartition)) {
      throw new IllegalArgumentException(
          "Expected DeltaInputPartition but got: " + partition.getClass().getName());
    }

    try {
      return new DeltaPartitionReaderOfRows((DeltaInputPartition) partition);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public PartitionReader<org.apache.spark.sql.vectorized.ColumnarBatch> createColumnarReader(
      InputPartition partition) {
    logger.info("createColumnarReader");

    if (!(partition instanceof DeltaInputPartition)) {
      throw new IllegalArgumentException(
          "Expected DeltaInputPartition but got: " + partition.getClass().getName());
    }

    try {
      return new DeltaPartitionReaderOfColumnarBatch((DeltaInputPartition) partition);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    logger.info("supportColumnarReads");
    return SparkSession.active()
        .sparkContext()
        .conf()
        .getBoolean("io.delta.kernel.spark.supportColumnarReads", true);
  }
}
