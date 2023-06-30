package io.delta.kernel.parquet;

import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import static io.delta.kernel.DefaultKernelUtils.findSubFieldType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;

import io.delta.kernel.types.LongType;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultColumnarBatch;
import io.delta.kernel.data.vector.DefaultStructVector;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

class RowConverter
    extends GroupConverter
    implements ParquetConverters.BaseConverter
{
    private final StructType readSchema;
    private final Converter[] converters;
    // The delta may request columns that don't exists in Parquet
    // This map is to track the ordinal known to Parquet reader to the converter array ordinal.
    // If a column is missing, a dummy converter added to the `converters` array and which
    // generates all null vector at the end.
    private final Map<Integer, Integer> parquetOrdinalToConverterOrdinal;

    // Working state
    private int currentRowIndex;
    private boolean[] nullability;

    /**
     * We have some necessary requirements here:
     * - the fields in fileSchema are a subset of readSchema (parquet schema has been pruned)
     * - the fields in fileSchema are in the same order as the corresponding fields in readSchema
     */
    RowConverter(
        int initialBatchSize,
        StructType readSchema,
        GroupType fileSchema)
    {
        this.readSchema = requireNonNull(readSchema, "readSchema is not null");
        List<StructField> fields = readSchema.fields();
        this.converters = new Converter[fields.size()];
        this.parquetOrdinalToConverterOrdinal = new HashMap<>();

        // Initialize the working state
        this.nullability = ParquetConverters.initNullabilityVector(initialBatchSize);

        int parquetOrdinal = 0;
        for (int i = 0; i < converters.length; i++) {
            final StructField field = fields.get(i);
            final DataType typeFromClient = field.getDataType();
            final Type typeFromFile = field.isDataColumn() ?
                    findSubFieldType(fileSchema, field) : null;
            if (typeFromFile == null) {
                if (field.getName() == StructField.ROW_INDEX_COLUMN_NAME &&
                        field.isMetadataColumn()) {
                    checkArgument(field.getDataType() instanceof LongType,
                            "row index metadata column must be type long");
                    converters[i] =
                            new ParquetConverters.FileRowIndexColumnConverter(initialBatchSize);
                } else {
                    converters[i] = new ParquetConverters.NonExistentColumnConverter(typeFromClient);
                }
            }
            else {
                converters[i] = ParquetConverters.createConverter(
                    initialBatchSize, typeFromClient, typeFromFile);
                parquetOrdinalToConverterOrdinal.put(parquetOrdinal, i);
                parquetOrdinal++;
            }
        }
    }

    @Override
    public Converter getConverter(int fieldIndex)
    {
        return converters[parquetOrdinalToConverterOrdinal.get(fieldIndex)];
    }

    @Override
    public void start()
    {
        Arrays.stream(converters)
            .filter(conv -> !conv.isPrimitive())
            .forEach(conv -> ((GroupConverter) conv).start());
    }

    @Override
    public void end()
    {
        Arrays.stream(converters)
            .filter(conv -> !conv.isPrimitive())
            .forEach(conv -> ((GroupConverter) conv).end());
    }

    public ColumnarBatch getDataAsColumnarBatch(int batchSize)
    {
        ColumnVector[] memberVectors = collectMemberVectors(batchSize);
        ColumnarBatch batch = new DefaultColumnarBatch(batchSize, readSchema, memberVectors);
        resetWorkingState();
        return batch;
    }

    /**
     * @return true if all members were null
     */
    private boolean moveConvertersToNextRow(Optional<Long> fileRowIndex) {
        long memberNullCount = Arrays.stream(converters)
                .map(converter -> (ParquetConverters.BaseConverter) converter)
                .map(converter -> {
                    if (fileRowIndex.isPresent() &&
                            converter instanceof ParquetConverters.FileRowIndexColumnConverter) {
                        return ((ParquetConverters.FileRowIndexColumnConverter) converter)
                                .moveToNextRow(fileRowIndex.get());
                    } else {
                        return converter.moveToNextRow();
                    }
                })
                .filter(result -> result)
                .count();
        return memberNullCount == converters.length;
    }

    /**
     * @param fileRowIndex the file row index of the row processed
     */
    public boolean moveToNextRow(long fileRowIndex) {
        resizeIfNeeded();

        boolean isNull = moveConvertersToNextRow(Optional.of(fileRowIndex));
        nullability[currentRowIndex] = isNull;

        currentRowIndex++;

        return isNull;
    }

    @Override
    public boolean moveToNextRow()
    {
        resizeIfNeeded();

        boolean isNull = moveConvertersToNextRow(Optional.empty());
        nullability[currentRowIndex] = isNull;

        currentRowIndex++;

        return isNull;
    }

    public ColumnVector getDataColumnVector(int batchSize)
    {
        ColumnVector[] memberVectors = collectMemberVectors(batchSize);
        ColumnVector vector = new DefaultStructVector(
            batchSize,
            readSchema,
            Optional.of(nullability),
            memberVectors
        );
        resetWorkingState();
        return vector;
    }

    private ColumnVector[] collectMemberVectors(int batchSize)
    {
        return Arrays.stream(converters)
            .map(converter -> ((ParquetConverters.BaseConverter) converter).getDataColumnVector(
                batchSize))
            .toArray(ColumnVector[]::new);
    }

    @Override
    public void resizeIfNeeded()
    {
        if (nullability.length == currentRowIndex) {
            int newSize = nullability.length * 2;
            this.nullability = Arrays.copyOf(this.nullability, newSize);
            ParquetConverters.setNullabilityToTrue(this.nullability, newSize / 2, newSize);
        }
    }

    @Override
    public void resetWorkingState()
    {
        this.currentRowIndex = 0;
        this.nullability = ParquetConverters.initNullabilityVector(this.nullability.length);
    }
}
