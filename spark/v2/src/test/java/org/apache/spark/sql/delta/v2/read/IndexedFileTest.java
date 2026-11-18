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
package org.apache.spark.sql.delta.v2.read;

import static org.apache.spark.sql.delta.v2.DeltaV2TestBase.createTestAddFile;
import static org.apache.spark.sql.delta.v2.DeltaV2TestBase.createTestCDCFile;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.sql.delta.actions.AddCDCFile;
import org.apache.spark.sql.delta.actions.AddFile;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IndexedFile}. */
public class IndexedFileTest {

  @Test
  public void testSentinel() {
    IndexedFile sentinel = IndexedFile.sentinel(/* version= */ 5L, /* index= */ -1L);

    assertEquals(5L, sentinel.getVersion());
    assertEquals(-1L, sentinel.getIndex());
    assertFalse(sentinel.hasFileAction());
    assertNull(sentinel.getAddFile());
    assertNull(sentinel.getCDCDataFile());
    assertFalse(sentinel.isAddCDCFile());
    assertThrows(IllegalStateException.class, sentinel::getFileSize);
    assertEquals("IndexedFile{version=5, index=-1}", sentinel.toString());
  }

  @Test
  public void testAddFile() {
    AddFile addFile = createTestAddFile("file.parquet", 4096, 100L);
    IndexedFile indexed = IndexedFile.addFile(/* version= */ 3L, /* index= */ 7L, addFile);

    assertEquals(3L, indexed.getVersion());
    assertEquals(7L, indexed.getIndex());
    assertTrue(indexed.hasFileAction());
    assertSame(addFile, indexed.getAddFile());
    assertNull(indexed.getCDCDataFile());
    assertFalse(indexed.isAddCDCFile());
    assertEquals(4096, indexed.getFileSize());

    String str = indexed.toString();
    assertTrue(str.startsWith("IndexedFile{version=3, index=7, addFile=AddFile("));
  }

  @Test
  public void testCdc() {
    // Inferred CDC (from AddFile)
    AddFile addFile = createTestAddFile("file.parquet", 3072, 100L);
    CDCDataFile inferredCdc = CDCDataFile.fromAddFile(addFile, /* commitTimestamp= */ 999L);
    IndexedFile inferred = IndexedFile.cdc(/* version= */ 2L, /* index= */ 0L, inferredCdc);

    assertTrue(inferred.hasFileAction());
    assertNull(inferred.getAddFile());
    assertSame(inferredCdc, inferred.getCDCDataFile());
    assertFalse(inferred.isAddCDCFile());
    assertEquals(3072, inferred.getFileSize());

    String str = inferred.toString();
    assertTrue(str.startsWith("IndexedFile{version=2, index=0, cdcDataFile=CDCDataFile{"));

    // Explicit CDC
    AddCDCFile cdcFile = createTestCDCFile("cdc-file.parquet", 5000);
    CDCDataFile explicitCdc = CDCDataFile.fromAddCDCFile(cdcFile, /* commitTimestamp= */ 888L);
    IndexedFile explicit = IndexedFile.cdc(/* version= */ 4L, /* index= */ 1L, explicitCdc);
    assertTrue(explicit.isAddCDCFile());
    assertEquals(5000, explicit.getFileSize());
  }
}
