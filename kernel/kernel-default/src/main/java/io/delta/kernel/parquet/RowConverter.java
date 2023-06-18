package io.delta.kernel.parquet;

import static io.delta.kernel.DefaultKernelUtils.findFieldType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
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

    RowConverter(
        int maxBatchSize,
        StructType readSchema,
        GroupType fileSchema)
    {
        this.readSchema = requireNonNull(readSchema, "readSchema is not null");
        List<StructField> fields = readSchema.fields();
        this.converters = new Converter[fields.size()];
        this.parquetOrdinalToConverterOrdinal = new HashMap<>();

        // Initialize the working state
        this.nullability = ParquetConverters.initNullabilityVector(maxBatchSize);

        int parquetOrdinal = 0;
        for (int i = 0; i < converters.length; i++) {
            final StructField field = fields.get(i);
            final DataType typeFromClient = field.getDataType();
            final Type typeFromFile = findFieldType(fileSchema, field);
            if (typeFromFile == null) {
                converters[i] = new ParquetConverters.NonExistentColumnConverter(typeFromClient);
            }
            else {
                converters[i] =
                    ParquetConverters.createConverter(maxBatchSize, typeFromClient, typeFromFile);
                parquetOrdinalToConverterOrdinal.put(parquetOrdinal, i);
                parquetOrdinal++;
            }
        }
    }

    @Override
    public Converter getConverter(int fieldIndex)
    {
        return converters[fieldIndex];
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

    @Override
    public boolean moveToNextRow()
    {
        resizeIfNeeded();
        long memberNullCount = Arrays.stream(converters)
            .map(converter -> (ParquetConverters.BaseConverter) converter)
            .map(converters -> converters.moveToNextRow())
            .filter(result -> result)
            .count();

        boolean isNull = memberNullCount == converters.length;
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

    private void resetWorkingState()
    {
        this.currentRowIndex = 0;
        this.nullability = ParquetConverters.initNullabilityVector(this.nullability.length);
    }
}
