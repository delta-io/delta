package io.delta.kernel.internal.replay;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Page Token Class for Pagination Support */
public class PageToken {

  public static PageToken fromRow(Row row) {
    requireNonNull(row);
    if (!PAGE_TOKEN_SCHEMA.equals(row.getSchema())) {
      throw new IllegalArgumentException(
          "Invalid Page Token: input row schema does not match expected PageToken schema. "
              + "Expected: "
              + PAGE_TOKEN_SCHEMA
              + ", Got: "
              + row.getSchema());
    }

    for (int i = 0; i < PAGE_TOKEN_SCHEMA.length(); i++) {
      if (row.isNullAt(i) && !Objects.equals(PAGE_TOKEN_SCHEMA.at(i).getName(), "sidecarIndex")) {
        throw new IllegalArgumentException(
            "Invalid Page Token: required field '"
                + PAGE_TOKEN_SCHEMA.at(i).getName()
                + "' is null at index "
                + i);
      }
    }

    return new PageToken(
        row.getString(0), // logFileName
        row.getLong(1), // rowIndexInFile
        Optional.ofNullable(row.isNullAt(2) ? null : row.getLong(2)), // sidecarIndex
        row.getString(3), // kernelVersion
        row.getString(4), // tablePath
        row.getLong(5), // tableVersion
        row.getLong(6), // predicateHash
        row.getLong(7)); // logSegmentHash
  }

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

  // ===== Variables to know where last page ends =====
  private final String startingFileName; // starting Log File Name (also last read log file name)
  private final long rowIndex; // the index of last row in the last consumed batch
  private final Optional<Long> sidecarIndex; // optional index of sidecar checkpoint file consumed

  // ===== Variables for validating query params =====
  private final String kernelVersion;
  private final String tablePath;
  private final long tableVersion;
  private final long predicateHash;
  private final long logSegmentHash;
  
  public PageToken(
      String startingFileName,
      long rowIndex,
      Optional<Long> sidecarIndex,
      String kernelVersion,
      String tablePath,
      long tableVersion,
      long predicateHash,
      long logSegmentHash) {
    this.startingFileName = requireNonNull(startingFileName, "startingFileName must not be null");
    this.rowIndex = rowIndex;
    this.sidecarIndex = sidecarIndex;
    this.kernelVersion = requireNonNull(kernelVersion, "kernelVersion must not be null");
    this.tablePath = requireNonNull(tablePath, "tablePath must not be null");
    this.tableVersion = tableVersion;
    this.predicateHash = predicateHash;
    this.logSegmentHash = logSegmentHash;
  }

  public Row toRow() {
    Map<Integer, Object> pageTokenMap = new HashMap<>();
    pageTokenMap.put(0, startingFileName);
    pageTokenMap.put(1, rowIndex);
    pageTokenMap.put(2, sidecarIndex.orElse(null));
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

  public Optional<Long> getSidecarIndex() {
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
        && tableVersion == other.tableVersion
        && predicateHash == other.predicateHash
        && logSegmentHash == other.logSegmentHash
        && Objects.equals(sidecarIndex, other.sidecarIndex)
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
