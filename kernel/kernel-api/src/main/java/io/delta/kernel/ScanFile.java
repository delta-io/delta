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
package io.delta.kernel;

import java.util.Map;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;

/**
 * Represents the metadata of {@link FilteredColumnarBatch} returned by
 * {@link Scan#getScanFiles(TableClient)}. It contains and the schema of the columnar batch
 * and methods to retrieve specific metadata values (e.x path in `AddFile` Delta Log action)
 * from each scan file row in the batch.
 */
public class ScanFile {
    /**
     * Schema of the scan file row returned by {@link Scan#getScanFiles(TableClient)}.
     * <p>
     * <ol>
     *  <li><ul>
     *   <li>name: {@code add}, type: {@code struct}</li>
     *   <li>Description: Represents `AddFile` DeltaLog action</li>
     *   <li><ul>
     *    <li>name: {@code path}, type: {@code string}, description: location of the file.</li>
     *    <li>name: {@code partitionValues}, type: {@code map(string, string)},
     *       description: A map from partition column to value for this logical file. </li>
     *    <li>name: {@code size}, type: {@code log}, description: size of the file.</li>
     *    <li>name: {@code modificationTime}, type: {@code log}, description: the time this
     *       logical file was created, as milliseconds since the epoch.</li>
     *    <li>name: {@code dataChange}, type: {@code boolean}, description: When false the
     *      logical file must already be present in the table or the records in the added file
     *      must be contained in one or more remove actions in the same version</li>
     *    <li>name: {@code deletionVector}, type: {@code string}, description: Either null
     *      (or absent in JSON) when no DV is associated with this data file, or a struct
     *      (described below) that contains necessary information about the DV that is part of
     *      this logical file. For description of each member variable in `deletionVector` @see
     *      <a href=https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vectors>
     *          Protocol</a><ul>
     *       <li>name: {@code storageType}, type: {@code string}</li>
     *       <li>name: {@code pathOrInlineDv}, type: {@code string}</li>
     *       <li>name: {@code offset}, type: {@code log}</li>
     *       <li>name: {@code sizeInBytes}, type: {@code log}</li>
     *       <li>name: {@code cardinality}, type: {@code log}</li>
     *    </ul></li>
     *   </ul></li>
     *  </ul></li>
     * </ol>
     */
    public static final StructType SCAN_FILE_SCHEMA = new StructType()
        .add("add", AddFile.SCHEMA)
        // TODO: table root is temporary, until the path in `add.path` is converted to
        // an absolute path.
        .add("tableRoot", StringType.INSTANCE);


    protected static final int ADD_FILE_ORDINAL = SCAN_FILE_SCHEMA.indexOf("add");

    protected static final StructType ADD_FILE_SCHEMA =
        (StructType) SCAN_FILE_SCHEMA.get("add").getDataType();

    protected static final int ADD_FILE_PATH_ORDINAL = ADD_FILE_SCHEMA.indexOf("path");

    protected static final int ADD_FILE_PARTITION_VALUES_ORDINAL =
        ADD_FILE_SCHEMA.indexOf("partitionValues");

    protected static final int ADD_FILE_SIZE_ORDINAL = ADD_FILE_SCHEMA.indexOf("size");

    protected static final int ADD_FILE_MOD_TIME_ORDINAL =
        ADD_FILE_SCHEMA.indexOf("modificationTime");

    protected static final int ADD_FILE_DATA_CHANGE_ORDINAL = ADD_FILE_SCHEMA.indexOf("dataChange");

    protected static final int ADD_FILE_DV_ORDINAL = ADD_FILE_SCHEMA.indexOf("deletionVector");

    protected static final String TABLE_ROOT_COL_NAME = "tableRoot";
    protected static final DataType TABLE_ROOT_DATA_TYPE = StringType.INSTANCE;
    protected static final int TABLE_ROOT_ORDINAL = SCAN_FILE_SCHEMA.indexOf(TABLE_ROOT_COL_NAME);

    /**
     * Get the {@link FileStatus} of {@code AddFile} from given scan file {@link Row}. The
     * {@link FileStatus} contains file metadata about the file.
     *
     * @param scanFileInfo {@link Row} representing one scan file.
     * @return a {@link FileStatus} object created from the given scan file row.
     */
    public static FileStatus getAddFileStatus(Row scanFileInfo) {
        Row addFile = getAddFileEntry(scanFileInfo);
        String path = addFile.getString(ADD_FILE_PATH_ORDINAL);
        Long size = addFile.getLong(ADD_FILE_SIZE_ORDINAL);
        Long modificationTime = addFile.getLong(ADD_FILE_MOD_TIME_ORDINAL);

        // TODO: this is hack until the path in `add.path` is converted to an absolute path
        String tableRoot = scanFileInfo.getString(TABLE_ROOT_ORDINAL);
        String absolutePath = new Path(tableRoot, path).toString();

        return FileStatus.of(absolutePath, size, modificationTime);
    }

    /**
     * Get the partition columns and values belonging to the {@code AddFile} from given scan file
     * row.
     *
     * @param scanFileInfo {@link Row} representing one scan file.
     * @return Map of partition column name to partition column value.
     */
    public static Map<String, String> getPartitionValues(Row scanFileInfo) {
        Row addFile = getAddFileEntry(scanFileInfo);
        return addFile.getMap(ADD_FILE_PARTITION_VALUES_ORDINAL);
    }



    /**
     * Helper method to get the {@code AddFile} struct from the scan file row.
     * @param scanFileInfo
     * @return {@link Row} representing the {@code AddFile}
     * @throws IllegalArgumentException If the scan file row doesn't contain {@code add} file entry.
     */
    protected static Row getAddFileEntry(Row scanFileInfo) {
        if (scanFileInfo.isNullAt(ADD_FILE_ORDINAL)) {
            throw new IllegalArgumentException("There is no `add` entry in the scan file row");
        }
        return scanFileInfo.getStruct(ADD_FILE_ORDINAL);
    }
}
