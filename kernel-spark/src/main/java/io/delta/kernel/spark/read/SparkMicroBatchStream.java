package io.delta.kernel.spark.read;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;

public class SparkMicroBatchStream implements MicroBatchStream {

  ////////////
  // offset //
  ////////////

  @Override
  public Offset initialOffset() {
    throw new UnsupportedOperationException("initialOffset is not supported");
  }

  @Override
  public Offset latestOffset() {
    throw new UnsupportedOperationException("latestOffset is not supported");
  }

  @Override
  public Offset deserializeOffset(String json) {
    throw new UnsupportedOperationException("deserializeOffset is not supported");
  }

  ////////////
  /// data ///
  ////////////

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    throw new UnsupportedOperationException("planInputPartitions is not supported");
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("createReaderFactory is not supported");
  }

  ///////////////
  // lifecycle //
  ///////////////

  @Override
  public void commit(Offset end) {
    throw new UnsupportedOperationException("commit is not supported");
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("stop is not supported");
  }
}
