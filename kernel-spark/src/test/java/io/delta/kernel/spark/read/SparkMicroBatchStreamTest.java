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
package io.delta.kernel.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparkMicroBatchStreamTest {

  private SparkMicroBatchStream microBatchStream;

  @BeforeEach
  void setUp() {
    Map<String, String> options = new HashMap<>();
    microBatchStream = new SparkMicroBatchStream("/path/to/test/table", options);
  }

  @Test
  public void testLatestOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.latestOffset());
    assertEquals("latestOffset is not supported", exception.getMessage());
  }

  @Test
  public void testPlanInputPartitions_throwsUnsupportedOperationException() {
    Offset start = null;
    Offset end = null;
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> microBatchStream.planInputPartitions(start, end));
    assertEquals("planInputPartitions is not supported", exception.getMessage());
  }

  @Test
  public void testCreateReaderFactory_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> microBatchStream.createReaderFactory());
    assertEquals("createReaderFactory is not supported", exception.getMessage());
  }

  @Test
  public void testInitialOffset_noLongerThrowsException() {
    // This test should be updated when initialOffset is properly implemented with table
    // dependencies
    // For now we just verify it doesn't throw UnsupportedOperationException anymore

    // Note: This will likely throw other exceptions since we don't have a valid table path
    // but it should not throw UnsupportedOperationException
    try {
      microBatchStream.initialOffset();
    } catch (UnsupportedOperationException e) {
      fail("initialOffset() should no longer throw UnsupportedOperationException");
    } catch (Exception e) {
      // Other exceptions are expected since we don't have proper dependencies
    }
  }

  @Test
  public void testDeserializeOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> microBatchStream.deserializeOffset("{}"));
    assertEquals("deserializeOffset is not supported", exception.getMessage());
  }

  @Test
  public void testCommit_throwsUnsupportedOperationException() {
    Offset end = null;
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.commit(end));
    assertEquals("commit is not supported", exception.getMessage());
  }

  @Test
  public void testStop_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.stop());
    assertEquals("stop is not supported", exception.getMessage());
  }

  @Test
  public void testInitialOffsetWithStartingVersionOption() {
    Map<String, String> options = new HashMap<>();
    options.put("startingVersion", "5");
    SparkMicroBatchStream stream = new SparkMicroBatchStream("/path/to/test/table", options);

    // This will fail with table not found, but should validate option parsing
    try {
      stream.initialOffset();
    } catch (IllegalArgumentException e) {
      // Expected since table doesn't exist
      assertTrue(e.getMessage().contains("Unable to load snapshot"));
    } catch (Exception e) {
      // Other exceptions might occur due to missing table
    }
  }

  @Test
  public void testInitialOffsetWithInvalidStartingVersion() {
    Map<String, String> options = new HashMap<>();
    options.put("startingVersion", "invalid");
    SparkMicroBatchStream stream = new SparkMicroBatchStream("/path/to/test/table", options);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> stream.initialOffset());
    assertTrue(exception.getMessage().contains("Invalid startingVersion"));
  }

  @Test
  public void testInitialOffsetWithStartingTimestampOption() {
    Map<String, String> options = new HashMap<>();
    options.put("startingTimestamp", "2023-01-01 00:00:00");
    SparkMicroBatchStream stream = new SparkMicroBatchStream("/path/to/test/table", options);

    // This will fail with table not found, but should validate option parsing
    try {
      stream.initialOffset();
    } catch (Exception e) {
      // Expected since table doesn't exist
    }
  }

  private static void assertTrue(boolean condition) {
    if (!condition) {
      fail("Assertion failed");
    }
  }
}
