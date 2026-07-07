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
package io.delta.kernel.defaults.internal.parquet;

import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.ReadSupport;

/**
 * Thin wrapper that exposes {@code parquet-mr}'s package-private {@code
 * InternalParquetRecordReader} to Kernel, delegating each call to the underlying reader via
 * reflection.
 *
 * <p>Kernel drives the low-level {@link ParquetFileReader} directly (rather than the high-level
 * {@link org.apache.parquet.hadoop.ParquetReader}) so that it can reuse the {@link
 * org.apache.parquet.hadoop.metadata.ParquetMetadata footer} it already read to build the row-group
 * filter, instead of having the reader read (and parse) the footer a second time. The class that
 * materializes rows from such a reader — {@code InternalParquetRecordReader} — is package-private,
 * so this wrapper reaches it reflectively.
 *
 * <p>An earlier version of this wrapper (delta-io/delta#4429) instead <em>subclassed</em> {@code
 * InternalParquetRecordReader} from a source file declared in package {@code
 * org.apache.parquet.hadoop}. That was removed because it caused {@link IllegalAccessError}s when
 * {@code parquet-mr} and Kernel are loaded by different class loaders: the JVM's package-private
 * access check requires the same runtime package (same package name <em>and</em> same class
 * loader), which does not hold across class loaders. Reflection with {@link
 * java.lang.reflect.AccessibleObject#setAccessible(boolean) setAccessible(true)} disables that
 * access check, so it works regardless of the class-loader topology.
 *
 * <p>Every member accessed here is {@code public} on {@code InternalParquetRecordReader}; only the
 * class itself is package-private.
 */
class ParquetRecordReaderWrapper<T> implements Closeable {

  private static final String INTERNAL_READER_CLASS =
      "org.apache.parquet.hadoop.InternalParquetRecordReader";

  // Reflection handles for InternalParquetRecordReader, resolved once and made accessible. They are
  // stateless and safe to share; the reader instances they operate on are not.
  private static final Constructor<?> CONSTRUCTOR;
  private static final Method INITIALIZE;
  private static final Method NEXT_KEY_VALUE;
  private static final Method GET_CURRENT_VALUE;
  private static final Method GET_CURRENT_ROW_INDEX;
  private static final Method CLOSE;

  static {
    try {
      Class<?> cls = Class.forName(INTERNAL_READER_CLASS);
      CONSTRUCTOR = cls.getConstructor(ReadSupport.class);
      INITIALIZE = cls.getMethod("initialize", ParquetFileReader.class, ParquetReadOptions.class);
      NEXT_KEY_VALUE = cls.getMethod("nextKeyValue");
      GET_CURRENT_VALUE = cls.getMethod("getCurrentValue");
      GET_CURRENT_ROW_INDEX = cls.getMethod("getCurrentRowIndex");
      CLOSE = cls.getMethod("close");
      // The class is package-private, so its public members are only reachable after this.
      CONSTRUCTOR.setAccessible(true);
      INITIALIZE.setAccessible(true);
      NEXT_KEY_VALUE.setAccessible(true);
      GET_CURRENT_VALUE.setAccessible(true);
      GET_CURRENT_ROW_INDEX.setAccessible(true);
      CLOSE.setAccessible(true);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(
          new IOException(
              "Failed to set up "
                  + INTERNAL_READER_CLASS
                  + ". This is likely due to an incompatible version of parquet-hadoop on the"
                  + " classpath.",
              e));
    }
  }

  // Thrown for an IllegalAccessException at a call site: it is effectively impossible after the
  // static block's setAccessible(true) calls succeeded, so it signals a broken JVM/parquet
  // environment rather than a recoverable I/O error.
  private static final String ACCESS_BUG_MESSAGE =
      "Unexpected reflective access failure on " + INTERNAL_READER_CLASS + " after setAccessible";

  private final Object internalReader;

  ParquetRecordReaderWrapper(ReadSupport<T> readSupport) throws IOException {
    requireNonNull(readSupport, "readSupport is null");
    try {
      this.internalReader = CONSTRUCTOR.newInstance(readSupport);
    } catch (InvocationTargetException e) {
      throw rethrow(e);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException(ACCESS_BUG_MESSAGE, e);
    }
  }

  /**
   * Delegates to {@code InternalParquetRecordReader.initialize(ParquetFileReader,
   * ParquetReadOptions)}.
   */
  void initialize(ParquetFileReader fileReader, ParquetReadOptions options) {
    try {
      INITIALIZE.invoke(internalReader, fileReader, options);
    } catch (InvocationTargetException e) {
      throw rethrowUnchecked(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(ACCESS_BUG_MESSAGE, e);
    }
  }

  /** Delegates to {@code InternalParquetRecordReader.nextKeyValue()}. */
  boolean nextKeyValue() throws IOException {
    try {
      return (Boolean) NEXT_KEY_VALUE.invoke(internalReader);
    } catch (InvocationTargetException e) {
      throw rethrow(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(ACCESS_BUG_MESSAGE, e);
    }
  }

  /** Delegates to {@code InternalParquetRecordReader.getCurrentValue()}. */
  @SuppressWarnings("unchecked")
  T getCurrentValue() throws IOException {
    try {
      return (T) GET_CURRENT_VALUE.invoke(internalReader);
    } catch (InvocationTargetException e) {
      throw rethrow(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(ACCESS_BUG_MESSAGE, e);
    }
  }

  /** Delegates to {@code InternalParquetRecordReader.getCurrentRowIndex()}. */
  long getCurrentRowIndex() {
    // Unlike the other methods, the underlying getCurrentRowIndex() declares no checked exception,
    // so this wrapper does not declare `throws IOException` either.
    try {
      return (Long) GET_CURRENT_ROW_INDEX.invoke(internalReader);
    } catch (InvocationTargetException e) {
      throw rethrowUnchecked(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(ACCESS_BUG_MESSAGE, e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      CLOSE.invoke(internalReader);
    } catch (InvocationTargetException e) {
      throw rethrow(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(ACCESS_BUG_MESSAGE, e);
    }
  }

  /**
   * Rethrows the real cause of a reflective call, preserving its type. {@link Error}s and {@link
   * RuntimeException}s (e.g. {@code ParquetDecodingException}) are rethrown as-is so callers see
   * the same exceptions they would from a direct call. An {@link IOException} cause is returned for
   * the caller to throw. Any other (checked) cause is wrapped in an {@link IOException}, which
   * mirrors {@link org.apache.parquet.hadoop.ParquetReader} that Kernel used previously.
   */
  private static IOException rethrow(InvocationTargetException e) {
    Throwable cause = e.getCause();
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    }
    if (cause instanceof IOException) {
      return (IOException) cause;
    }
    return new IOException(cause);
  }

  /**
   * Variant of {@link #rethrow} for methods that do not declare {@code throws IOException}. The
   * underlying method declares no checked exception, so a checked cause would itself be a bug; wrap
   * it in an unchecked exception.
   */
  private static RuntimeException rethrowUnchecked(InvocationTargetException e) {
    Throwable cause = e.getCause();
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    }
    return new RuntimeException(cause);
  }
}
