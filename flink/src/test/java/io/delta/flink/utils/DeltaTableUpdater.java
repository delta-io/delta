package io.delta.flink.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import io.delta.flink.utils.TestDescriptor.Descriptor;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;

/**
 * This class inserts new data into Delta table.
 */
public class DeltaTableUpdater {

    private static final String ENGINE_INFO = "local";

    private static final Configuration configuration = DeltaTestUtils.getHadoopConf();

    private final String deltaTablePath;

    public DeltaTableUpdater(String deltaTablePath) {
        this.deltaTablePath = deltaTablePath;
    }

    /**
     * Writes records to Delta table accordingly to {@link Descriptor}. All new data from {@link
     * Descriptor} will be inserted into Delta table under one commit, creating one new Delta
     * version for entire {@link Descriptor}.
     */
    public void writeToTable(Descriptor descriptor) {
        List<Row> rows = descriptor.getRows();
        RowType rowType = descriptor.getRowType();

        try {
            long now = System.currentTimeMillis();
            DeltaLog deltaLog = DeltaLog.forTable(configuration, deltaTablePath);

            Path pathToParquet = writeToParquet(deltaTablePath, rowType, rows);

            AddFile addFile =
                AddFile.builder(pathToParquet.getPath(), Collections.emptyMap(), rows.size(), now,
                        true)
                    .build();

            // Commit Delta transaction.
            // Start new Delta transaction.
            OptimisticTransaction txn = deltaLog.startTransaction();
            Operation op = new Operation(Operation.Name.WRITE);
            txn.commit(Collections.singletonList(addFile), op, ENGINE_INFO);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes Rows into Parquet files.
     *
     * @param deltaTablePath Root folder under which a Parquet file should be created.
     * @param rowType        A {@link RowType} describing column types for rows.
     * @param rows           A {@link List} of rows to write into the Parquet file.
     * @return A {@link Path} to created Parquet file.
     * @throws IOException {@link IOException} in case of any IO issue during writing to Parquet
     *                     file.
     */
    private Path writeToParquet(String deltaTablePath, RowType rowType, List<Row> rows)
        throws IOException {

        ParquetWriterFactory<RowData> factory =
            ParquetRowDataBuilder.createWriterFactory(rowType, configuration, false);

        Path path = new Path(deltaTablePath, UUID.randomUUID().toString());
        BulkWriter<RowData> writer =
            factory.create(path.getFileSystem().create(path, WriteMode.OVERWRITE));

        DataFormatConverter<RowData, Row> converter = getConverter(rowType);
        for (Row row : rows) {
            writer.addElement(converter.toInternal(row));
        }

        writer.flush();
        writer.finish();

        return path;
    }

    @SuppressWarnings("unchecked")
    private DataFormatConverter<RowData, Row> getConverter(RowType rowType) {
        return (DataFormatConverter<RowData, Row>) DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(rowType));
    }

}
