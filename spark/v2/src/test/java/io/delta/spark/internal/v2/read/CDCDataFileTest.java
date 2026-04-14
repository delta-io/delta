/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.read;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddCDCFile;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.GenerateIcebergCompatActionUtils;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CDCDataFile}. */
public class CDCDataFileTest {

  private static final StructType EMPTY_SCHEMA = new StructType();

  private static AddFile createTestAddFile(String path, long size, long modificationTime) {
    return new AddFile(
        AddFile.createAddFileRow(
            EMPTY_SCHEMA,
            path,
            VectorUtils.stringStringMapValue(Collections.emptyMap()),
            size,
            modificationTime,
            /* dataChange= */ true,
            /* deletionVector= */ Optional.empty(),
            /* tags= */ Optional.empty(),
            /* baseRowId= */ Optional.empty(),
            /* defaultRowCommitVersion= */ Optional.empty(),
            /* stats= */ Optional.empty()));
  }

  private static RemoveFile createTestRemoveFile(String path, long size) {
    Row row =
        GenerateIcebergCompatActionUtils.createRemoveFileRowWithExtendedFileMetadata(
            path,
            /* deletionTimestamp= */ 100L,
            /* dataChange= */ true,
            VectorUtils.stringStringMapValue(Collections.emptyMap()),
            size,
            /* stats= */ Optional.empty(),
            /* physicalSchema= */ null,
            /* baseRowId= */ Optional.empty(),
            /* defaultRowCommitVersion= */ Optional.empty(),
            /* deletionVector= */ Optional.empty());
    return new RemoveFile(row);
  }

  private static Row createCDCRow(long size) {
    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(AddCDCFile.FULL_SCHEMA.indexOf("path"), "cdc-file.parquet");
    fieldMap.put(
        AddCDCFile.FULL_SCHEMA.indexOf("partitionValues"),
        VectorUtils.stringStringMapValue(Collections.emptyMap()));
    fieldMap.put(AddCDCFile.FULL_SCHEMA.indexOf("size"), size);
    return new GenericRow(AddCDCFile.FULL_SCHEMA, fieldMap);
  }

  @Test
  public void testFromAddFile() {
    AddFile addFile = createTestAddFile("file1.parquet", 2048, 100L);
    CDCDataFile cdcFile = CDCDataFile.fromAddFile(addFile, /* commitTimestamp= */ 12345L);

    assertSame(addFile, cdcFile.getAddFile());
    assertEquals(CDCReader.CDC_TYPE_INSERT(), cdcFile.getChangeType());
    assertEquals(12345L, cdcFile.getCommitTimestamp());
    assertEquals(2048, cdcFile.getFileSize());
    assertFalse(cdcFile.isAddCDCFile());
  }

  @Test
  public void testFromRemoveFile() {
    RemoveFile removeFile = createTestRemoveFile("removed.parquet", 4096);
    CDCDataFile cdcFile = CDCDataFile.fromRemoveFile(removeFile, /* commitTimestamp= */ 99999L);

    assertNull(cdcFile.getAddFile());
    assertEquals(CDCReader.CDC_TYPE_DELETE_STRING(), cdcFile.getChangeType());
    assertEquals(99999L, cdcFile.getCommitTimestamp());
    assertEquals(4096, cdcFile.getFileSize());
    assertFalse(cdcFile.isAddCDCFile());
  }

  @Test
  public void testFromAddCDCFile() {
    Row cdcRow = createCDCRow(/* size= */ 8192);
    CDCDataFile cdcFile = CDCDataFile.fromAddCDCFile(cdcRow, /* commitTimestamp= */ 55555L);

    assertNull(cdcFile.getAddFile());
    assertNull(cdcFile.getChangeType());
    assertEquals(55555L, cdcFile.getCommitTimestamp());
    assertEquals(8192, cdcFile.getFileSize());
    assertTrue(cdcFile.isAddCDCFile());
  }
}
