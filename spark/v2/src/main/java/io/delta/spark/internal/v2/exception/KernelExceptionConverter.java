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

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.exceptions.UnsupportedProtocolVersionException;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.InvalidProtocolVersionException;
import org.apache.spark.sql.delta.actions.Action$;

/**
 * Translates Delta Kernel exceptions into the Spark/Delta exceptions.
 *
 * <p>Mappings live in a class-to-handler registry, lookup walks the exception's class hierarchy and
 * kernel exceptions without a registered handler are returned unchanged. To add a mapping, register
 * one more handler below.
 */
public final class KernelExceptionConverter {

  private KernelExceptionConverter() {}

  @FunctionalInterface
  private interface Handler {
    Throwable translate(KernelException e, String tableNameOrPath, Operation op);
  }

  private static final Map<Class<? extends KernelException>, Handler> HANDLERS = new HashMap<>();

  static {
    HANDLERS.put(TableNotFoundException.class, KernelExceptionConverter::translateTableNotFound);
    HANDLERS.put(
        UnsupportedProtocolVersionException.class,
        KernelExceptionConverter::translateUnsupportedProtocolVersion);
  }

  /** Returns the Spark/Delta equivalent of {@code e}, or {@code e} unchanged. */
  public static Throwable convert(KernelException e, String tableNameOrPath, Operation op) {
    for (Class<?> c = e.getClass();
        c != null && KernelException.class.isAssignableFrom(c);
        c = c.getSuperclass()) {
      Handler handler = HANDLERS.get(c);
      if (handler != null) {
        return handler.translate(e, tableNameOrPath, op);
      }
    }
    return e;
  }

  /** Converts {@code e} and throws the result. */
  public static RuntimeException translateAndThrow(
      KernelException e, String tableNameOrPath, Operation op) {
    throw sneakyThrow(convert(e, tableNameOrPath, op));
  }

  private static Throwable translateTableNotFound(
      KernelException e, String tableNameOrPath, Operation op) {
    return op == Operation.TABLE_RESOLUTION
        ? DeltaErrors.schemaNotSetException()
        : DeltaErrors.pathNotExistsException(tableNameOrPath);
  }

  private static Throwable translateUnsupportedProtocolVersion(
      KernelException e, String tableNameOrPath, Operation op) {
    UnsupportedProtocolVersionException source = (UnsupportedProtocolVersionException) e;
    boolean isReader =
        source.getVersionType() == UnsupportedProtocolVersionException.ProtocolVersionType.READER;
    return new InvalidProtocolVersionException(
        tableNameOrPath,
        isReader ? source.getVersion() : 0,
        isReader ? 0 : source.getVersion(),
        Action$.MODULE$.supportedReaderVersionNumbers().toList(),
        Action$.MODULE$.supportedWriterVersionNumbers().toList());
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> E sneakyThrow(Throwable t) throws E {
    throw (E) t;
  }
}
