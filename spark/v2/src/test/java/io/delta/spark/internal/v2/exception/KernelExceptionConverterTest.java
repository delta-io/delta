/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.exceptions.UnsupportedProtocolVersionException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.delta.InvalidProtocolVersionException;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link KernelExceptionConverter}. */
public class KernelExceptionConverterTest {

  private static final String PATH = "/tmp/some/table";

  @Test
  public void tableNotFound_tableResolution_mapsToSchemaNotSet() {
    Throwable translated =
        KernelExceptionConverter.convert(
            new TableNotFoundException(PATH), PATH, Operation.TABLE_RESOLUTION);

    AnalysisException ae = assertInstanceOf(AnalysisException.class, translated);
    assertTrue(
        ae.getMessage().contains("Table schema is not set"),
        "expected 'Table schema is not set', got: " + ae.getMessage());
    assertTrue(
        ae.getMessage().contains("CREATE TABLE"),
        "expected 'CREATE TABLE', got: " + ae.getMessage());
  }

  @Test
  public void tableNotFound_microBatch_mapsToPathDoesNotExist() {
    Throwable translated =
        KernelExceptionConverter.convert(
            new TableNotFoundException(PATH), PATH, Operation.STREAMING_MICROBATCH);

    AnalysisException ae = assertInstanceOf(AnalysisException.class, translated);
    assertTrue(
        ae.getMessage().contains(PATH) && ae.getMessage().contains("doesn't exist"),
        "expected a path-does-not-exist message, got: " + ae.getMessage());
  }

  @Test
  public void unsupportedReaderProtocol_mapsToInvalidProtocolVersion() {
    int badVersion = Integer.MAX_VALUE;
    Throwable translated =
        KernelExceptionConverter.convert(
            new UnsupportedProtocolVersionException(
                PATH, badVersion, UnsupportedProtocolVersionException.ProtocolVersionType.READER),
            PATH,
            Operation.STREAMING_MICROBATCH);

    InvalidProtocolVersionException ipve =
        assertInstanceOf(InvalidProtocolVersionException.class, translated);
    assertEquals(PATH, ipve.tableNameOrPath());
    assertEquals(badVersion, ipve.readerRequiredVersion());
    assertTrue(
        ipve.supportedReaderVersions().nonEmpty(), "expected non-empty supported reader versions");
  }

  @Test
  public void unregisteredKernelException_returnedUnchanged() {
    KernelException original = new KernelException("some unmapped kernel failure");
    Throwable translated =
        KernelExceptionConverter.convert(original, PATH, Operation.TABLE_RESOLUTION);

    assertSame(original, translated, "unmapped Kernel exceptions must pass through unchanged");
  }

  @Test
  public void translateAndThrow_throwsTranslatedException() {
    assertThrows(
        InvalidProtocolVersionException.class,
        () ->
            KernelExceptionConverter.translateAndThrow(
                new UnsupportedProtocolVersionException(
                    PATH,
                    Integer.MAX_VALUE,
                    UnsupportedProtocolVersionException.ProtocolVersionType.WRITER),
                PATH,
                Operation.STREAMING_MICROBATCH));
  }
}
