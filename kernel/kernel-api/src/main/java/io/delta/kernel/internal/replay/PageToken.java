package io.delta.kernel.internal.replay;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Page Token Class for Pagination Support */
public class PageToken {
  /** Variables to know where last page ends (current page starts) */
  private final String startingFileName; // starting Log File Name (also last read log file name)

  private final long rowIndex; // the index of last row in the last consumed batch
  private final long sidecarIndex; // the index of sidecar checkpoint file consumed

  /** Variables for validating query params */
  private final String kernelVersion;

  private final String tablePath;
  private final long tableVersion;
  private final long predicateHash;
  private final long logSegmentHash;

  /** Schema for PageToken Row representation */
  public static final StructType PAGE_TOKEN_SCHEMA =
      new StructType()
          .add("logFileName", StringType.STRING)
          .add("rowIndexInFile", LongType.LONG)
          .add("sidecarIndex", LongType.LONG)
          .add("kernelVersion", StringType.STRING)
          .add("tablePath", StringType.STRING)
          .add("tableVersion", LongType.LONG)
          .add("predicateHash", LongType.LONG)
          .add("logSegmentHash", LongType.LONG);

  public PageToken(
      String startingFileName,
      long rowIndex,
      long sidecarIndex,
      String kernelVersion,
      String tablePath,
      long tableVersion,
      long predicateHash,
      long logSegmentHash) {
    this.startingFileName =
        Objects.requireNonNull(startingFileName, "startingFileName must not be null");
    this.rowIndex = rowIndex;
    this.sidecarIndex = sidecarIndex;
    this.kernelVersion = Objects.requireNonNull(kernelVersion, "kernelVersion must not be null");
    this.tablePath = Objects.requireNonNull(tablePath, "tablePath must not be null");
    this.tableVersion = tableVersion;
    this.predicateHash = predicateHash;
    this.logSegmentHash = logSegmentHash;
  }

  public static PageToken fromRow(Row row) {
    StructType inputSchema = row.getSchema();
    if (!PAGE_TOKEN_SCHEMA.equals(inputSchema)) {
      throw new IllegalArgumentException(
          "Invalid Page Token: input row schema does not match expected PageToken schema. "
              + "Expected: "
              + PAGE_TOKEN_SCHEMA
              + ", Got: "
              + inputSchema);
    }

    for (int i = 0; i < 8; i++) {
      if (row.isNullAt(i)) {
        throw new IllegalArgumentException("Invalid Page Token: field at index " + i + " is null");
      }
    }

    return new PageToken(
        row.getString(0), // logFileName
        row.getLong(1), // rowIndexInFile
        row.getLong(2), // sidecarIndex
        row.getString(3), // kernelVersion
        row.getString(4), // tablePath
        row.getLong(5), // tableVersion
        row.getLong(6), // predicateHash
        row.getLong(7)); // logSegmentHash
  }

  public Row toRow() {
    Map<Integer, Object> pageTokenMap = new HashMap<>();
    pageTokenMap.put(0, startingFileName);
    pageTokenMap.put(1, rowIndex);
    pageTokenMap.put(2, sidecarIndex);
    pageTokenMap.put(3, kernelVersion);
    pageTokenMap.put(4, tablePath);
    pageTokenMap.put(5, tableVersion);
    pageTokenMap.put(6, predicateHash);
    pageTokenMap.put(7, logSegmentHash);

    return new GenericRow(PAGE_TOKEN_SCHEMA, pageTokenMap);
  }

  public String getStartingFileName() {
    return startingFileName;
  }

  public long getRowIndex() {
    return rowIndex;
  }

  public long getSidecarIndex() {
    return sidecarIndex;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    PageToken other = (PageToken) obj;

    return rowIndex == other.rowIndex
        && sidecarIndex == other.sidecarIndex
        && tableVersion == other.tableVersion
        && predicateHash == other.predicateHash
        && logSegmentHash == other.logSegmentHash
        && Objects.equals(startingFileName, other.startingFileName)
        && Objects.equals(kernelVersion, other.kernelVersion)
        && Objects.equals(tablePath, other.tablePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        startingFileName,
        rowIndex,
        sidecarIndex,
        kernelVersion,
        tablePath,
        tableVersion,
        predicateHash,
        logSegmentHash);
  }
}
