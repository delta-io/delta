/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.defaults.internal.parquet;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultDecimalVector;
import io.delta.kernel.defaults.internal.parquet.ParquetColumnReaders.BasePrimitiveColumnReader;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DecimalType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Decimal column readers for materializing the column values from Parquet files into Kernels {@link
 * ColumnVector}.
 */
public class DecimalColumnReader {

  public static Converter createDecimalConverter(
      int initialBatchSize, DecimalType typeFromClient, Type typeFromFile) {

    PrimitiveType primType = typeFromFile.asPrimitiveType();
    LogicalTypeAnnotation typeAnnotation = primType.getLogicalTypeAnnotation();

    if (primType.getPrimitiveTypeName() == INT32) {
      // For INT32 backed decimals
      if (typeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) typeAnnotation;
        return new IntDictionaryAwareDecimalColumnReader(
            typeFromClient, decimalType.getPrecision(), decimalType.getScale(), initialBatchSize);
      } else {
        // If the column is a plain INT32, we should pick the precision that can host
        // the largest INT32 value.
        return new IntDictionaryAwareDecimalColumnReader(typeFromClient, 10, 0, initialBatchSize);
      }
    } else if (primType.getPrimitiveTypeName() == INT64) {
      // For INT64 backed decimals
      if (typeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) typeAnnotation;
        return new LongDictionaryAwareDecimalColumnReader(
            typeFromClient, decimalType.getPrecision(), decimalType.getScale(), initialBatchSize);
      } else {
        // If the column is a plain INT64, we should pick the precision that can host
        // the largest INT64 value.
        return new LongDictionaryAwareDecimalColumnReader(typeFromClient, 20, 0, initialBatchSize);
      }
    } else if (primType.getPrimitiveTypeName() == FIXED_LEN_BYTE_ARRAY
        || primType.getPrimitiveTypeName() == BINARY) {
      // For BINARY and FIXED_LEN_BYTE_ARRAY backed decimals
      if (typeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) typeAnnotation;
        return new BinaryDictionaryAwareDecimalColumnReader(
            typeFromClient, decimalType.getPrecision(), decimalType.getScale(), initialBatchSize);
      } else {
        throw new RuntimeException(
            String.format(
                "Unable to create Parquet converter for DecimalType whose parquet "
                    + "type is %s without decimal metadata.",
                typeFromFile));
      }
    } else {
      throw new RuntimeException(
          String.format(
              "Unable to create Parquet converter for DecimalType whose Parquet type "
                  + "is %s. Parquet DECIMAL type can only be backed by INT32, INT64, "
                  + "FIXED_LEN_BYTE_ARRAY, or BINARY",
              typeFromFile));
    }
  }

  public abstract static class BaseDecimalColumnReader extends BasePrimitiveColumnReader {
    // working state
    private BigDecimal[] values;

    private final DecimalType decimalType;

    private final int scale;
    protected BigDecimal[] expandedDictionary;

    BaseDecimalColumnReader(DataType dataType, int precision, int scale, int initialBatchSize) {
      super(initialBatchSize);
      DecimalType decimalType = (DecimalType) dataType;
      int scaleIncrease = decimalType.getScale() - scale;
      int precisionIncrease = decimalType.getPrecision() - precision;
      checkArgument(
          scaleIncrease >= 0 && precisionIncrease >= scaleIncrease,
          "Found Delta type %s but Parquet type has precision=%s and scale=%s",
          decimalType,
          precision,
          scale);
      this.scale = scale;
      this.decimalType = decimalType;
      this.values = new BigDecimal[initialBatchSize];
    }

    /**
     * Dictionary support is an optional optimization to reduce BigDecimal instantiation and binary
     * decoding (for Binary backed decimals).
     */
    @Override
    public boolean hasDictionarySupport() {
      return true;
    }

    protected void addDecimal(BigDecimal value) {
      resizeIfNeeded();
      this.nullability[currentRowIndex] = false;
      if (decimalType.getScale() != scale) {
        value = value.setScale(decimalType.getScale(), RoundingMode.UNNECESSARY);
      }
      this.values[currentRowIndex] = value;
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      addDecimal(expandedDictionary[dictionaryId]);
    }

    @Override
    public ColumnVector getDataColumnVector(int batchSize) {
      ColumnVector vector = new DefaultDecimalVector(decimalType, batchSize, values);
      // re-initialize the working space
      this.nullability = ParquetColumnReaders.initNullabilityVector(nullability.length);
      this.values = new BigDecimal[values.length];
      this.currentRowIndex = 0;
      return vector;
    }

    @Override
    public void resizeIfNeeded() {
      if (values.length == currentRowIndex) {
        int newSize = values.length * 2;
        this.values = Arrays.copyOf(this.values, newSize);
        this.nullability = Arrays.copyOf(this.nullability, newSize);
        ParquetColumnReaders.setNullabilityToTrue(this.nullability, newSize / 2, newSize);
      }
    }

    protected BigDecimal decimalFromLong(long value) {
      return BigDecimal.valueOf(value, scale);
    }

    protected BigDecimal decimalFromBinary(Binary value) {
      return new BigDecimal(new BigInteger(value.getBytes()), scale);
    }
  }

  public static class IntDictionaryAwareDecimalColumnReader extends BaseDecimalColumnReader {
    IntDictionaryAwareDecimalColumnReader(
        DataType dataType, int precision, int scale, int initialBatchSize) {
      super(dataType, precision, scale, initialBatchSize);
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      this.expandedDictionary = new BigDecimal[dictionary.getMaxId() + 1];
      for (int id = 0; id < dictionary.getMaxId() + 1; id++) {
        this.expandedDictionary[id] = decimalFromLong(dictionary.decodeToInt(id));
      }
    }

    @Override
    // Converts decimals stored as INT32
    public void addInt(int value) {
      addDecimal(decimalFromLong(value));
    }
  }

  public static class LongDictionaryAwareDecimalColumnReader extends BaseDecimalColumnReader {
    LongDictionaryAwareDecimalColumnReader(
        DataType dataType, int precision, int scale, int initialBatchSize) {
      super(dataType, precision, scale, initialBatchSize);
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      this.expandedDictionary = new BigDecimal[dictionary.getMaxId() + 1];
      for (int id = 0; id < dictionary.getMaxId() + 1; id++) {
        this.expandedDictionary[id] = decimalFromLong(dictionary.decodeToLong(id));
      }
    }

    @Override
    // Converts decimals stored as INT64
    public void addLong(long value) {
      addDecimal(decimalFromLong(value));
    }
  }

  public static class BinaryDictionaryAwareDecimalColumnReader extends BaseDecimalColumnReader {
    BinaryDictionaryAwareDecimalColumnReader(
        DataType dataType, int precision, int scale, int initialBatchSize) {
      super(dataType, precision, scale, initialBatchSize);
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      this.expandedDictionary = new BigDecimal[dictionary.getMaxId() + 1];
      for (int id = 0; id < dictionary.getMaxId() + 1; id++) {
        this.expandedDictionary[id] = decimalFromBinary(dictionary.decodeToBinary(id));
      }
    }

    @Override
    // Converts decimals stored as either FIXED_LENGTH_BYTE_ARRAY or BINARY
    public void addBinary(Binary value) {
      addDecimal(decimalFromBinary(value));
    }
  }
}
