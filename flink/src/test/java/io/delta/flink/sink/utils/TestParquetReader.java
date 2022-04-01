/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.sink.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;

/**
 * Provides utility methods for reading back test records written to Parquet files.
 */
public class TestParquetReader {

    /**
     * This test method resolves all parquet files in the current snapshot of DeltaLake table, next
     * it reads those files and try to parse every record back as {@link org.apache.flink.types.Row}
     * object. If the parsing doesn't succeed then an exception will be thrown, otherwise the record
     * counter will be incremented and the validation will skip to the next row till the end of the
     * file.
     *
     * @param deltaLog {@link DeltaLog} instance representing table for which the validation should
     *                 be run
     * @return number of read and successfully validated records in the table
     * @throws IOException Thrown when the data cannot be read or writer cannot be instantiated
     */
    public static int readAndValidateAllTableRecords(DeltaLog deltaLog) throws IOException {
        List<AddFile> deltaTableFiles = deltaLog.snapshot().getAllFiles();
        int cumulatedRecords = 0;
        for (AddFile addedFile : deltaTableFiles) {
            Path parquetFilePath = new Path(deltaLog.getPath().toString(), addedFile.getPath());
            cumulatedRecords += TestParquetReader.parseAndCountRecords(
                parquetFilePath, DeltaSinkTestUtils.TEST_ROW_TYPE);
        }
        return cumulatedRecords;
    }

    /**
     * Reads and counts all records in given Parquet file. Additionally, it tries to parse back
     * every record to the format provided as {@link RowType}.
     *
     * @param parquetFilepath path to the file
     * @param rowType         Flink's logical type that will be used for parsing back data read
     *                        from Parquet file
     * @return count of written records
     * @throws IOException Thrown if an error occurs while reading the file
     */
    public static int parseAndCountRecords(Path parquetFilepath,
                                           RowType rowType) throws IOException {
        ParquetColumnarRowSplitReader reader = getTestParquetReader(
            parquetFilepath,
            rowType
        );

        int recordsRead = 0;
        while (!reader.reachedEnd()) {
            DeltaSinkTestUtils.CONVERTER.toExternal(reader.nextRecord());
            recordsRead++;
        }
        return recordsRead;
    }

    private static ParquetColumnarRowSplitReader getTestParquetReader(
        Path path, RowType rowType) throws IOException {
        return ParquetSplitReaderUtil.genPartColumnarRowReader(
            true, // utcTimestamp
            true, // caseSensitive
            DeltaSinkTestUtils.getHadoopConf(),
            rowType.getFieldNames().toArray(new String[0]),
            rowType.getChildren().stream()
                .map(TypeConversions::fromLogicalToDataType)
                .toArray(DataType[]::new),
            new HashMap<>(),
            IntStream.range(0, rowType.getFieldCount()).toArray(),
            50,
            path,
            0,
            Long.MAX_VALUE);
    }
}
