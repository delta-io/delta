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
package io.delta.kernel.internal;

import java.util.Collections;
import java.util.HashMap;

import io.delta.kernel.ScanFile;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.StructField;

import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.GenericRow;

/**
 * Internal extension of {@link ScanFile} contains non-public APIs.
 */
public class InternalScanFile extends ScanFile {
    private InternalScanFile() {}

    public static StructField TABLE_ROOT_STRUCT_FIELD = new StructField(
        TABLE_ROOT_COL_NAME,
        TABLE_ROOT_DATA_TYPE,
        false, /* nullable */
        Collections.emptyMap());

    /**
     * Create a scan file row conforming to the schema {@link ScanFile#SCAN_FILE_SCHEMA} for
     * given file status. This is used when creating the ScanFile row for reading commit or
     * checkpoint files.
     *
     * @param fileStatus
     * @return
     */
    public static Row generateScanFileRow(FileStatus fileStatus) {
        Row addFile = new GenericRow(
            ADD_FILE_SCHEMA,
            new HashMap<Integer, Object>() {
                {
                    put(ADD_FILE_PATH_ORDINAL, fileStatus.getPath());
                    put(ADD_FILE_PARTITION_VALUES_ORDINAL, null); // partitionValues
                    put(ADD_FILE_SIZE_ORDINAL, fileStatus.getSize());
                    put(ADD_FILE_MOD_TIME_ORDINAL, fileStatus.getModificationTime());
                    put(ADD_FILE_DATA_CHANGE_ORDINAL, null); /* dataChange */
                    put(ADD_FILE_DV_ORDINAL, null); /* deletionVector */
                }
            });

        return new GenericRow(
            SCAN_FILE_SCHEMA,
            new HashMap<Integer, Object>() {
                {
                    put(ADD_FILE_ORDINAL, addFile);
                    put(TABLE_ROOT_ORDINAL, "/");
                }
            });
    }

    /**
     * Create a {@link DeletionVectorDescriptor} from {@code add} entry in the given scan file row.
     * @param scanFile {@link Row} representing one scan file.
     * @return
     */
    public static DeletionVectorDescriptor getDeletionVectorDescriptorFromRow(Row scanFile) {
        Row addFile = getAddFileEntry(scanFile);
        return DeletionVectorDescriptor.fromRow(addFile.getStruct(ADD_FILE_DV_ORDINAL));
    }
}
