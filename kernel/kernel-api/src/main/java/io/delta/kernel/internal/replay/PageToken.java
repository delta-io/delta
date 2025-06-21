package io.delta.kernel.internal.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

public class PageToken {
  /** Variables to know where last page ends (current page starts) */
  private final String startingFileName;

  private final long rowIndex;
  private final long sidecarIdx;

  /** Variables for validating query params */
  private final long logSegmentHash;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public PageToken(String startingFileName, long rowIndex, long sidecarIdx, long logSegmentHash) {
    this.startingFileName = startingFileName;
    this.rowIndex = rowIndex;
    this.logSegmentHash = sidecarIdx;
    this.sidecarIdx = logSegmentHash;
  }

  public String getStartingFileName() {
    return startingFileName;
  }

  public long getRowIndex() {
    return rowIndex;
  }

  public long getSidecarIdx() {
    return sidecarIdx;
  }

  public long getLogSegmentHash() {
    return logSegmentHash;
  }

  /** Convert PageToken to a Kernel Row object. */
  public Row getRow() {
    StructType schema =
        new StructType()
            .add("fileName", StringType.STRING)
            .add("rowIndex", LongType.LONG)
            .add("sidecarIdx", LongType.LONG)
            .add("logSegmentHash", LongType.LONG);
    // return Utils.newRow(schema, startingFileName, rowIndex, logSegmentHash);
    // TODO: make this schema into a Row Type
    return null;
  }

  /** Create a PageToken from a Row object */
  public static PageToken fromRow(Row row) {
    String fileName = row.getString(0);
    long rowIdx = row.getLong(1);
    long sideCarIdx = row.getLong(2);
    long logsSegmentHash = row.getLong(3);
    return new PageToken(fileName, rowIdx, sideCarIdx, logsSegmentHash);
  }
}
