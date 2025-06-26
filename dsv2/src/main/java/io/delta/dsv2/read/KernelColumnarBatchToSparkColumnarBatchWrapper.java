package io.delta.dsv2.read;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KernelColumnarBatchToSparkColumnarBatchWrapper
    extends org.apache.spark.sql.vectorized.ColumnarBatch {

  private static final Logger logger =
      LoggerFactory.getLogger(KernelColumnarBatchToSparkColumnarBatchWrapper.class);

  public KernelColumnarBatchToSparkColumnarBatchWrapper(
      org.apache.spark.sql.vectorized.ColumnVector[] columns, int numRows) {
    super(columns, numRows);
  }

  /**
   * Factory method to create a KernelColumnarBatchToSparkColumnarBatchWrapper from a
   * KernelFilteredColumnarBatch. This replaces the Scala 'apply' method.
   */
  public static KernelColumnarBatchToSparkColumnarBatchWrapper create(
      FilteredColumnarBatch kernelFilteredColumnarBatch) {

    io.delta.kernel.data.ColumnarBatch kernelColumnarBatch = kernelFilteredColumnarBatch.getData();
    int numColumns = kernelColumnarBatch.getSchema().length();
    int numRows = kernelColumnarBatch.getSize();

    Optional<int[]> desiredToActualRowIdMapping = Optional.empty();
    if (kernelFilteredColumnarBatch.getSelectionVector().isPresent()) {
      ColumnVector selectionVector = kernelFilteredColumnarBatch.getSelectionVector().get();
      List<Integer> rowIdMapping = new ArrayList<>();

      for (int actualRowId = 0; actualRowId < selectionVector.getSize(); actualRowId++) {
        if (!selectionVector.isNullAt(actualRowId) && selectionVector.getBoolean(actualRowId)) {
          rowIdMapping.add(actualRowId);
          logger.info("rowIdMapping index " + (rowIdMapping.size() - 1) + " -> " + actualRowId);
        } else {
          logger.info("skipping actual selection vector row " + actualRowId);
        }
      }

      logger.info("rowIdMapping size: " + rowIdMapping.size());
      logger.info("number of skipped rows: " + (selectionVector.getSize() - rowIdMapping.size()));

      numRows = rowIdMapping.size();
      int[] mappingArray = new int[rowIdMapping.size()];
      for (int i = 0; i < rowIdMapping.size(); i++) {
        mappingArray[i] = rowIdMapping.get(i);
      }
      desiredToActualRowIdMapping = Optional.of(mappingArray);
    }

    logger.info(
        "kernelColumnarBatch: numRows "
            + numRows
            + ", numColumns: "
            + numColumns
            + ", "
            + "getSchema "
            + kernelColumnarBatch.getSchema());

    org.apache.spark.sql.vectorized.ColumnVector[] columns =
        new org.apache.spark.sql.vectorized.ColumnVector[numColumns];

    for (int i = 0; i < numColumns; i++) {
      logger.info(
          "Creating SparkColumnVector for column "
              + i
              + ": "
              + kernelColumnarBatch.getColumnVector(i).getDataType());

      columns[i] =
          new KernelColumnVectorToSparkColumnVectorWrapper(
              kernelColumnarBatch.getColumnVector(i), desiredToActualRowIdMapping);
    }

    return new KernelColumnarBatchToSparkColumnarBatchWrapper(columns, numRows);
  }
}
