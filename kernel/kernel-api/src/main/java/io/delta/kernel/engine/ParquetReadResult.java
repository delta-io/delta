package io.delta.kernel.engine;

import io.delta.kernel.data.ColumnarBatch;

public class ParquetReadResult {

  private final ColumnarBatch data;
  private final String fileName;

  public ParquetReadResult(ColumnarBatch data, String fileName) {
    this.data = data;
    this.fileName = fileName;
  }
  public ColumnarBatch getData(){
    return this.data;
  }

  public  String getFileName(){
    return this.fileName;
  }
}
