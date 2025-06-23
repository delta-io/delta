package io.delta.kernel.engine;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.fs.Path;

public class ParquetReadResult {

  private final ColumnarBatch data;
  private final Path filePath;

  public ParquetReadResult(ColumnarBatch data, Path filePath) {
    this.data = data;
    this.filePath = filePath;
  }

  public ColumnarBatch getData() {
    return this.data;
  }

  public Path getFilePath() {
    return this.filePath;
  }
}
