package io.delta.kernel.engine;

import io.delta.kernel.data.ColumnarBatch;

/**
 * Represents the result of reading a batch of data in a Parquet file.
 *
 * <p>Encapsulates both the data read (as a {@link ColumnarBatch}) and the full path of the file *
 * from which the data was read.
 */
public class ParquetReadResult {

  private final ColumnarBatch data;
  private final String filePath;

  /**
   * Constructs a {@code ParquetReadResult} object with the given data and file path.
   *
   * @param data the columnar batch of data read from the file
   * @param filePath the path of the file from which the data was read
   */
  public ParquetReadResult(ColumnarBatch data, String filePath) {
    this.data = data;
    this.filePath = filePath;
  }

  /**
   * Returns the {@link ColumnarBatch} of data that was read from the file.
   *
   * @return the columnar data
   */
  public ColumnarBatch getData() {
    return data;
  }

  /**
   * Returns the path of the Parquet file that this data was read from.
   *
   * @return the full file path as a string
   */
  public String getFilePath() {
    return filePath;
  }
}
