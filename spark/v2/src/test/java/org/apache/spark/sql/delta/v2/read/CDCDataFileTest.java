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
import static org.apache.spark.sql.delta.v2.DeltaV2TestBase.createTestRemoveFile;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.delta.RowIndexFilterType;
import org.apache.spark.sql.delta.actions.AddCDCFile;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;
import org.apache.spark.sql.delta.actions.RemoveFile;
import org.apache.spark.sql.delta.commands.cdc.CDCReader;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.JavaConverters;

/** Unit tests for {@link CDCDataFile}. */
public class CDCDataFileTest {

  private static final DeletionVectorDescriptor TEST_DV =
      new DeletionVectorDescriptor(
          "u", "ab^-rstxgiarsd", Option.apply((Object) 4), 40, 3L, Option.empty());

  private static AddCDCFile createCDCFile(long size) {
    return createTestCDCFile("cdc-file.parquet", size);
  }

  private static AddCDCFile createCDCFile(long size, Map<String, String> partitionValues) {
    return createTestCDCFile("cdc-file.parquet", size, partitionValues);
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
    assertSame(removeFile, cdcFile.getRemoveFile());
    assertEquals(CDCReader.CDC_TYPE_DELETE_STRING(), cdcFile.getChangeType());
    assertEquals(99999L, cdcFile.getCommitTimestamp());
    assertEquals(4096, cdcFile.getFileSize());
    assertFalse(cdcFile.isAddCDCFile());
  }

  @Test
  public void testGetRemoveFile_nullForAddFile() {
    AddFile addFile = createTestAddFile("file.parquet", 1024, 100L);
    CDCDataFile cdcFile = CDCDataFile.fromAddFile(addFile, /* commitTimestamp= */ 100L);
    assertNull(cdcFile.getRemoveFile());
  }

  @Test
  public void testHasDeletionVector_addFileWithDV() {
    AddFile addFile = createTestAddFile("file.parquet", 1024, 100L, Optional.of(TEST_DV));
    CDCDataFile cdcFile = CDCDataFile.fromAddFile(addFile, /* commitTimestamp= */ 100L);
    assertTrue(cdcFile.hasDeletionVector());
  }

  @Test
  public void testHasDeletionVector_addFileWithoutDV() {
    AddFile addFile = createTestAddFile("file.parquet", 1024, 100L);
    CDCDataFile cdcFile = CDCDataFile.fromAddFile(addFile, /* commitTimestamp= */ 100L);
    assertFalse(cdcFile.hasDeletionVector());
  }

  @Test
  public void testHasDeletionVector_removeFileWithDV() {
    RemoveFile removeFile = createTestRemoveFile("file.parquet", 1024, Optional.of(TEST_DV));
    CDCDataFile cdcFile = CDCDataFile.fromRemoveFile(removeFile, /* commitTimestamp= */ 100L);
    assertTrue(cdcFile.hasDeletionVector());
  }

  @Test
  public void testHasDeletionVector_removeFileWithoutDV() {
    RemoveFile removeFile = createTestRemoveFile("file.parquet", 1024);
    CDCDataFile cdcFile = CDCDataFile.fromRemoveFile(removeFile, /* commitTimestamp= */ 100L);
    assertFalse(cdcFile.hasDeletionVector());
  }

  @Test
  public void testHasDeletionVector_addCDCFile() {
    AddCDCFile cdcFileAction = createCDCFile(/* size= */ 8192);
    CDCDataFile cdcFile = CDCDataFile.fromAddCDCFile(cdcFileAction, /* commitTimestamp= */ 100L);
    assertFalse(cdcFile.hasDeletionVector());
  }

  @Test
  public void testFromAddCDCFile() {
    AddCDCFile cdcFileAction = createCDCFile(/* size= */ 8192);
    CDCDataFile cdcFile = CDCDataFile.fromAddCDCFile(cdcFileAction, /* commitTimestamp= */ 55555L);

    assertNull(cdcFile.getAddFile());
    assertNull(cdcFile.getChangeType());
    assertEquals(55555L, cdcFile.getCommitTimestamp());
    assertEquals(8192, cdcFile.getFileSize());
    assertTrue(cdcFile.isAddCDCFile());
  }

  @Test
  public void testCDCPartitionValuesPreserveNullAndEmptyValues() {
    Map<String, String> expected = new HashMap<>();
    expected.put("country", "US");
    expected.put("empty", "");
    expected.put("nullable", null);
    assertPartitionValues(expected);
  }

  @Test
  public void testEmptyCDCPartitionValues() {
    assertPartitionValues(Collections.emptyMap());
  }

  private static void assertPartitionValues(Map<String, String> expected) {
    CDCDataFile cdcFile = CDCDataFile.fromAddCDCFile(createCDCFile(8192, expected), 55555L);
    assertEquals(expected, JavaConverters.mapAsJavaMap(cdcFile.getPartitionValues()));
    assertEquals("cdc-file.parquet", cdcFile.getPath());
    assertEquals(8192, cdcFile.getFileSize());
    assertEquals(55555L, cdcFile.getCommitTimestamp());
  }

  @Test
  public void testFromDVDiff_delete() {
    AddFile addFile = createTestAddFile("data.parquet", 1024, 100L, Optional.of(TEST_DV));
    CDCDataFile cdcFile =
        CDCDataFile.fromDVDiff(
            addFile,
            CDCReader.CDC_TYPE_DELETE_STRING(),
            /* commitTimestamp= */ 77777L,
            "inline-dv==");

    assertSame(addFile, cdcFile.getAddFile());
    assertEquals(CDCReader.CDC_TYPE_DELETE_STRING(), cdcFile.getChangeType());
    assertEquals(77777L, cdcFile.getCommitTimestamp());
    assertEquals(1024, cdcFile.getFileSize());
    assertFalse(cdcFile.isAddCDCFile());
    assertEquals(Optional.of("inline-dv=="), cdcFile.getEffectiveDv());
    assertEquals(RowIndexFilterType.IF_NOT_CONTAINED, cdcFile.getDvFilterType());
    assertEquals("data.parquet", cdcFile.getPath());
    assertTrue(cdcFile.hasDeletionVector());
  }

  @Test
  public void testFromDVDiff_insert() {
    AddFile addFile = createTestAddFile("data.parquet", 2048, 200L, Optional.of(TEST_DV));
    CDCDataFile cdcFile =
        CDCDataFile.fromDVDiff(
            addFile, CDCReader.CDC_TYPE_INSERT(), /* commitTimestamp= */ 88888L, "restore-dv==");

    assertEquals(CDCReader.CDC_TYPE_INSERT(), cdcFile.getChangeType());
    assertEquals(Optional.of("restore-dv=="), cdcFile.getEffectiveDv());
    assertEquals(RowIndexFilterType.IF_NOT_CONTAINED, cdcFile.getDvFilterType());
  }
}
