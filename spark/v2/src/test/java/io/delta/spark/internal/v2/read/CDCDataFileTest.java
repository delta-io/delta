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

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import java.util.Collections;
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

  @Test
  public void testFromAddFile() {
    AddFile addFile = createTestAddFile("file1.parquet", 2048, 100L);
    CDCDataFile cdcFile = CDCDataFile.fromAddFile(addFile, /* commitTimestamp= */ 12345L);

    assertSame(addFile, cdcFile.getAddFile());
    assertEquals(CDCReader.CDC_TYPE_INSERT(), cdcFile.getChangeType());
    assertEquals(12345L, cdcFile.getCommitTimestamp());
    assertEquals(2048, cdcFile.getFileSize());

    String str = cdcFile.toString();
    assertTrue(str.startsWith("CDCDataFile{addFile=AddFile{"));
    assertTrue(str.contains("changeType='insert', commitTimestamp=12345}"));
  }
}
