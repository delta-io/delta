package io.delta.kernel.internal.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

public class PageToken {
  /** Variables to know where last page ends (current page starts) */
  private final String startingFileName;
  private final long rowIndex;

  /** TODO: Variables for validating query params */

  public PageToken(String startingFileName, long rowIndex) {
    this.startingFileName = startingFileName;
    this.rowIndex = rowIndex;
  }

  public String getStartingFileName() {
    return startingFileName;
  }

  public long getRowIndex() {
    return rowIndex;
  }

  /** Convert PageToken to a Kernel Row object. */
  public Row getRow() {
    StructType schema =
        new StructType()
            .add("fileName", StringType.STRING)
            .add("rowIndex", LongType.LONG);
    // return Utils.newRow(schema, startingFileName, rowIndex, logSegmentHash);
    // TODO: make this schema into a Row Type
    return null;
  }

  /** Create a PageToken from a Row object */
  public static PageToken fromRow(Row row) {
    String fileName = row.getString(0);
    long rowIdx = row.getLong(1);
    return new PageToken(fileName, rowIdx);
  }
}
