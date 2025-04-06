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
package io.delta.kernel.internal.skipping;

import static io.delta.kernel.internal.util.ColumnMapping.getPhysicalName;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Provides information and utilities for statistics columns given a table schema. Specifically, it
 * is used to:
 *
 * <ol>
 *   <li>Get the expected statistics schema given a table schema
 *   <li>Check if a {@link Literal} or {@link Column} is skipping eligible
 *   <li>Get the statistics column for a given stat type and logical column
 * </ol>
 */
public class StatsSchemaHelper {

  //////////////////////////////////////////////////////////////////////////////////
  // Public static fields and methods
  //////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns true if the given literal is skipping-eligible. Delta tracks min/max stats for a
   * limited set of data types and only literals of those types are skipping eligible.
   */
  public static boolean isSkippingEligibleLiteral(Literal literal) {
    return isSkippingEligibleDataType(literal.getDataType());
  }

  /** Returns true if the given data type is eligible for MIN/MAX data skipping. */
  public static boolean isSkippingEligibleDataType(DataType dataType) {
    return SKIPPING_ELIGIBLE_TYPE_NAMES.contains(dataType.toString())
        ||
        // DecimalType is eligible but since its string includes scale + precision it needs to
        // be matched separately
        dataType instanceof DecimalType;
  }

  /**
   * Returns the expected statistics schema given a table schema.
   *
   * <p>Here is an example of a data schema along with the schema of the statistics that would be
   * collected.
   *
   * <p>Data Schema: {{{ |-- a: struct (nullable = true) | |-- b: struct (nullable = true) | | |--
   * c: long (nullable = true) }}}
   *
   * <p>Collected Statistics: {{{ |-- stats: struct (nullable = true) | |-- numRecords: long
   * (nullable = false) | |-- minValues: struct (nullable = false) | | |-- a: struct (nullable =
   * false) | | | |-- b: struct (nullable = false) | | | | |-- c: long (nullable = true) | |--
   * maxValues: struct (nullable = false) | | |-- a: struct (nullable = false) | | | |-- b: struct
   * (nullable = false) | | | | |-- c: long (nullable = true) | |-- nullCount: struct (nullable =
   * false) | | |-- a: struct (nullable = false) | | | |-- b: struct (nullable = false) | | | | |--
   * c: long (nullable = true) }}}
   */
  public static StructType getStatsSchema(StructType dataSchema) {
    StructType statsSchema = new StructType().add(NUM_RECORDS, LongType.LONG, true);
    StructType minMaxStatsSchema = getMinMaxStatsSchema(dataSchema);
    if (minMaxStatsSchema.length() > 0) {
      statsSchema =
          statsSchema
              .add(MIN, getMinMaxStatsSchema(dataSchema), true)
              .add(MAX, getMinMaxStatsSchema(dataSchema), true);
    }
    StructType nullCountSchema = getNullCountSchema(dataSchema);
    if (nullCountSchema.length() > 0) {
      statsSchema = statsSchema.add(NULL_COUNT, getNullCountSchema(dataSchema), true);
    }
    return statsSchema;
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Instance fields and public methods
  //////////////////////////////////////////////////////////////////////////////////

  private final StructType dataSchema;
  /* Map of all leaf columns from logical to physical names */
  private final Map<Column, Column> logicalToPhysicalColumn;
  /* Map of all leaf logical columns to their data type */
  private final Map<Column, DataType> logicalToDataType;

  public StatsSchemaHelper(StructType dataSchema) {
    this.dataSchema = dataSchema;
    Map<Column, Tuple2<Column, DataType>> logicalToPhysicalColumnAndDataType =
        getLogicalToPhysicalColumnAndDataType(dataSchema);
    this.logicalToPhysicalColumn =
        logicalToPhysicalColumnAndDataType.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, e -> e.getValue()._1 // map to just the column
                    ));
    this.logicalToDataType =
        logicalToPhysicalColumnAndDataType.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, e -> e.getValue()._2 // map to just the data type
                    ));
  }

  /**
   * Given a logical column in the data schema provided when creating {@code this}, return the
   * corresponding MIN column and an optional column adjustment expression from the statistic schema
   * that stores the MIN values for the provided logical column.
   *
   * @param column the logical column name.
   * @return a tuple of the MIN column and an optional adjustment expression.
   */
  public Tuple2<Column, Optional<Expression>> getMinColumn(Column column) {
    checkArgument(
        isSkippingEligibleMinMaxColumn(column),
        "%s is not a valid min column for data schema %s",
        column,
        dataSchema);
    return new Tuple2<>(getStatsColumn(column, MIN), Optional.empty());
  }

  /**
   * Given a logical column in the data schema provided when creating {@code this}, return the
   * corresponding MAX column and an optional column adjustment expression from the statistic schema
   * that stores the MAX values for the provided logical column.
   *
   * @param column the logical column name.
   * @return a tuple of the MAX column and an optional adjustment expression.
   */
  public Tuple2<Column, Optional<Expression>> getMaxColumn(Column column) {
    checkArgument(
        isSkippingEligibleMinMaxColumn(column),
        "%s is not a valid min column for data schema %s",
        column,
        dataSchema);
    DataType dataType = logicalToDataType.get(column);
    Column maxColumn = getStatsColumn(column, MAX);

    // If this is a column of type Timestamp or TimestampNTZ
    // compensate for the truncation from microseconds to milliseconds
    // by adding 1 millisecond. For example, a file containing only
    // 01:02:03.456789 will be written with min == max == 01:02:03.456, so we must consider it
    // to contain the range from 01:02:03.456 to 01:02:03.457.
    if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
      return new Tuple2<>(
          maxColumn,
          Optional.of(
              new ScalarExpression("TIMEADD", Arrays.asList(maxColumn, Literal.ofLong(1)))));
    }
    return new Tuple2<>(maxColumn, Optional.empty());
  }

  /**
   * Given a logical column in the data schema provided when creating {@code this}, return the
   * corresponding NULL_COUNT column in the statistic schema that stores the null count values for
   * the provided logical column.
   */
  public Column getNullCountColumn(Column column) {
    checkArgument(
        isSkippingEligibleNullCountColumn(column),
        "%s is not a valid null_count column for data schema %s",
        column,
        dataSchema);
    return getStatsColumn(column, NULL_COUNT);
  }

  /** Returns the NUM_RECORDS column in the statistic schema */
  public Column getNumRecordsColumn() {
    return new Column(NUM_RECORDS);
  }

  /**
   * Returns true if the given column is skipping-eligible using min/max statistics. This means the
   * column exists, is a leaf column, and is of a skipping-eligible data-type.
   */
  public boolean isSkippingEligibleMinMaxColumn(Column column) {
    return logicalToDataType.containsKey(column)
        && isSkippingEligibleDataType(logicalToDataType.get(column));
  }

  /**
   * Returns true if the given column is skipping-eligible using null count statistics. This means
   * the column exists and is a leaf column as we only collect stats for leaf columns.
   */
  public boolean isSkippingEligibleNullCountColumn(Column column) {
    return logicalToPhysicalColumn.containsKey(column);
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Private static fields and methods
  //////////////////////////////////////////////////////////////////////////////////

  /* Delta statistics field names for file statistics */
  private static final String NUM_RECORDS = "numRecords";
  private static final String MIN = "minValues";
  private static final String MAX = "maxValues";
  private static final String NULL_COUNT = "nullCount";

  private static final Set<String> SKIPPING_ELIGIBLE_TYPE_NAMES =
      new HashSet<String>() {
        {
          add("byte");
          add("short");
          add("integer");
          add("long");
          add("float");
          add("double");
          add("date");
          add("timestamp");
          add("timestamp_ntz");
          add("string");
        }
      };

  /**
   * Given a data schema returns the expected schema for a min or max statistics column. This means
   * 1) replace logical names with physical names 2) set nullable=true 3) only keep stats eligible
   * fields (i.e. don't include fields with isSkippingEligibleDataType=false)
   */
  private static StructType getMinMaxStatsSchema(StructType dataSchema) {
    List<StructField> fields = new ArrayList<>();
    for (StructField field : dataSchema.fields()) {
      if (isSkippingEligibleDataType(field.getDataType())) {
        fields.add(new StructField(getPhysicalName(field), field.getDataType(), true));
      } else if (field.getDataType() instanceof StructType) {
        fields.add(
            new StructField(
                getPhysicalName(field),
                getMinMaxStatsSchema((StructType) field.getDataType()),
                true));
      }
    }
    return new StructType(fields);
  }

  /**
   * Given a data schema returns the expected schema for a null_count statistics column. This means
   * 1) replace logical names with physical names 2) set nullable=true 3) use LongType for all
   * fields
   */
  private static StructType getNullCountSchema(StructType dataSchema) {
    List<StructField> fields = new ArrayList<>();
    for (StructField field : dataSchema.fields()) {
      if (field.getDataType() instanceof StructType) {
        fields.add(
            new StructField(
                getPhysicalName(field),
                getNullCountSchema((StructType) field.getDataType()),
                true));
      } else {
        fields.add(new StructField(getPhysicalName(field), LongType.LONG, true));
      }
    }
    return new StructType(fields);
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Private class helpers
  //////////////////////////////////////////////////////////////////////////////////

  /**
   * Given a logical column and a stats type returns the corresponding column in the statistics
   * schema
   */
  private Column getStatsColumn(Column column, String statType) {
    checkArgument(
        logicalToPhysicalColumn.containsKey(column),
        "%s is not a valid leaf column for data schema: %s",
        column,
        dataSchema);
    return getChildColumn(logicalToPhysicalColumn.get(column), statType);
  }

  /**
   * Given a data schema returns a map of {logical column -> (physical column, data type)} for all
   * leaf columns in the schema.
   */
  private Map<Column, Tuple2<Column, DataType>> getLogicalToPhysicalColumnAndDataType(
      StructType dataSchema) {
    Map<Column, Tuple2<Column, DataType>> result = new HashMap<>();
    for (StructField field : dataSchema.fields()) {
      if (field.getDataType() instanceof StructType) {
        Map<Column, Tuple2<Column, DataType>> nestedCols =
            getLogicalToPhysicalColumnAndDataType((StructType) field.getDataType());
        for (Column childLogicalCol : nestedCols.keySet()) {
          Tuple2<Column, DataType> childCol = nestedCols.get(childLogicalCol);
          Column childPhysicalCol = childCol._1;
          DataType childColDataType = childCol._2;
          result.put(
              getChildColumn(childLogicalCol, field.getName()),
              new Tuple2<>(
                  getChildColumn(childPhysicalCol, getPhysicalName(field)), childColDataType));
        }
      } else {
        result.put(
            new Column(field.getName()),
            new Tuple2<>(new Column(getPhysicalName(field)), field.getDataType()));
      }
    }
    return result;
  }

  /** Returns the provided column as a child column nested under {@code parentName} */
  private static Column getChildColumn(Column column, String parentName) {
    return new Column(prependArray(column.getNames(), parentName));
  }

  /**
   * Given an array {@code names} and a string element {@code preElem} return a new array with
   * {@code preElem} inserted at the beginning
   */
  private static String[] prependArray(String[] arr, String preElem) {
    String[] newNames = new String[arr.length + 1];
    newNames[0] = preElem;
    System.arraycopy(arr, 0, newNames, 1, arr.length);
    return newNames;
  }
}
