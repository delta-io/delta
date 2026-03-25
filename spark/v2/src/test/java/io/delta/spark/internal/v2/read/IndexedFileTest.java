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
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IndexedFile}. */
public class IndexedFileTest {

  private static final StructType EMPTY_SCHEMA = new StructType();

  private static AddFile createTestAddFile(String path, long size) {
    return new AddFile(
        AddFile.createAddFileRow(
            EMPTY_SCHEMA,
            path,
            VectorUtils.stringStringMapValue(Collections.emptyMap()),
            size,
            /* modificationTime= */ 100L,
            /* dataChange= */ true,
            /* deletionVector= */ Optional.empty(),
            /* tags= */ Optional.empty(),
            /* baseRowId= */ Optional.empty(),
            /* defaultRowCommitVersion= */ Optional.empty(),
            /* stats= */ Optional.empty()));
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
  public void testSentinel() {
    IndexedFile sentinel = IndexedFile.sentinel(/* version= */ 5L, /* index= */ -1L);

    assertEquals(5L, sentinel.getVersion());
    assertEquals(-1L, sentinel.getIndex());
    assertFalse(sentinel.hasFileAction());
    assertNull(sentinel.getAddFile());
    assertNull(sentinel.getCDCFile());
    assertFalse(sentinel.isExplicitCDCFile());
    assertThrows(IllegalStateException.class, sentinel::getFileSize);
    assertEquals("IndexedFile{version=5, index=-1}", sentinel.toString());
  }

  @Test
  public void testAddFile() {
    AddFile addFile = createTestAddFile("file.parquet", 4096);
    IndexedFile indexed = IndexedFile.addFile(/* version= */ 3L, /* index= */ 7L, addFile);

    assertEquals(3L, indexed.getVersion());
    assertEquals(7L, indexed.getIndex());
    assertTrue(indexed.hasFileAction());
    assertSame(addFile, indexed.getAddFile());
    assertNull(indexed.getCDCFile());
    assertFalse(indexed.isExplicitCDCFile());
    assertEquals(4096, indexed.getFileSize());

    String str = indexed.toString();
    assertTrue(str.startsWith("IndexedFile{version=3, index=7, addFile=AddFile{"));
  }

  @Test
  public void testCdc() {
    // Inferred CDC (from AddFile)
    AddFile addFile = createTestAddFile("file.parquet", 3072);
    CDCDataFile inferredCdc = CDCDataFile.fromAddFile(addFile, /* commitTimestamp= */ 999L);
    IndexedFile inferred = IndexedFile.cdc(/* version= */ 2L, /* index= */ 0L, inferredCdc);

    assertTrue(inferred.hasFileAction());
    assertNull(inferred.getAddFile());
    assertSame(inferredCdc, inferred.getCDCFile());
    assertFalse(inferred.isExplicitCDCFile());
    assertEquals(3072, inferred.getFileSize());

    String str = inferred.toString();
    assertTrue(str.startsWith("IndexedFile{version=2, index=0, cdcFile=CDCDataFile{"));

    // Explicit CDC (from AddCDCFile row)
    Row cdcRow = createCDCRow(/* size= */ 5000);
    CDCDataFile explicitCdc = CDCDataFile.fromExplicitCDC(cdcRow, /* commitTimestamp= */ 888L);
    IndexedFile explicit = IndexedFile.cdc(/* version= */ 4L, /* index= */ 1L, explicitCdc);
    assertTrue(explicit.isExplicitCDCFile());
    assertEquals(5000, explicit.getFileSize());
  }
}
