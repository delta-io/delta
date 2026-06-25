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
package io.delta.spark.internal.v2.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import javax.annotation.Nullable;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.ColumnDefaultValue;
import org.apache.spark.sql.connector.catalog.IdentityColumnSpec;
import org.apache.spark.sql.internal.connector.ColumnImpl;
import org.apache.spark.sql.types.DataType;

/**
 * Spark-version compatibility helpers for DSv2 {@link Column} column IDs.
 *
 * <p>Column IDs ({@code Column#id()}) and the 9-arg {@link ColumnImpl} constructor are available on
 * newer Spark builds. Reflection keeps the same source compatible with Spark CI builds that may not
 * yet expose {@code Column#id()}.
 */
public final class ColumnV2Utils {
  private static final boolean COLUMN_ID_SUPPORTED = detectColumnIdSupport();

  private ColumnV2Utils() {}

  /** Returns true when the runtime Spark build supports {@link Column#id()}. */
  public static boolean supportsColumnId() {
    return COLUMN_ID_SUPPORTED;
  }

  /** Creates a DSv2 column, attaching {@code columnId} when the runtime Spark build supports it. */
  public static Column createColumn(
      String name, DataType dataType, boolean nullable, @Nullable String columnId) {
    if (columnId != null && COLUMN_ID_SUPPORTED) {
      return newColumnWithId(name, dataType, nullable, columnId);
    }
    return newColumnWithoutId(name, dataType, nullable);
  }

  /** Returns {@link Column#id()} when supported, otherwise null. */
  @Nullable
  public static String getColumnId(Column column) {
    if (!COLUMN_ID_SUPPORTED) {
      return null;
    }
    try {
      Method idMethod = Column.class.getMethod("id");
      return (String) idMethod.invoke(column);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to read Column.id()", e);
    }
  }

  private static Column newColumnWithoutId(String name, DataType dataType, boolean nullable) {
    try {
      Constructor<ColumnImpl> ctor =
          ColumnImpl.class.getConstructor(
              String.class,
              DataType.class,
              boolean.class,
              String.class,
              ColumnDefaultValue.class,
              String.class,
              IdentityColumnSpec.class,
              String.class);
      return ctor.newInstance(
          name,
          dataType,
          nullable,
          /* comment = */ null,
          /* defaultValue = */ null,
          /* generationExpression = */ null,
          /* identityColumnSpec = */ null,
          /* metadataInJSON = */ null);
    } catch (NoSuchMethodException e) {
      return newColumnWithId(name, dataType, nullable, /* columnId = */ null);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to create ColumnImpl without column id", e);
    }
  }

  private static Column newColumnWithId(
      String name, DataType dataType, boolean nullable, String columnId) {
    try {
      Constructor<ColumnImpl> ctor =
          ColumnImpl.class.getConstructor(
              String.class,
              DataType.class,
              boolean.class,
              String.class,
              ColumnDefaultValue.class,
              String.class,
              IdentityColumnSpec.class,
              String.class,
              String.class);
      return ctor.newInstance(
          name,
          dataType,
          nullable,
          /* comment = */ null,
          /* defaultValue = */ null,
          /* generationExpression = */ null,
          /* identityColumnSpec = */ null,
          /* metadataInJSON = */ null,
          columnId);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to create ColumnImpl with column id", e);
    }
  }

  private static boolean detectColumnIdSupport() {
    try {
      Column.class.getMethod("id");
      ColumnImpl.class.getConstructor(
          String.class,
          DataType.class,
          boolean.class,
          String.class,
          ColumnDefaultValue.class,
          String.class,
          IdentityColumnSpec.class,
          String.class,
          String.class);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
