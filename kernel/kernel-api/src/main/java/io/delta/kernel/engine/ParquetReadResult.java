package io.delta.kernel.engine;

import io.delta.kernel.data.ColumnarBatch;

public class ParquetReadResult {

  private final ColumnarBatch data;
  private final String filePath;

  public ParquetReadResult(ColumnarBatch data, String filePath) {
    this.data = data;
    this.filePath = filePath;
  }

  public ColumnarBatch getData() {
    return this.data;
  }

  public String getFilePath() {
    return this.filePath;
  }
}
