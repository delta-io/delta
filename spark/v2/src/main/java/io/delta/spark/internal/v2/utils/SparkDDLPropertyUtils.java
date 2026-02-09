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
package io.delta.spark.internal.v2.utils;

import org.apache.spark.sql.connector.catalog.TableCatalog;

/**
 * Identifies Spark DSv2 reserved table property keys that should not be persisted into the Delta
 * log's {@code metadata.configuration}.
 */
public final class SparkDDLPropertyUtils {

  private SparkDDLPropertyUtils() {}

  /**
   * Returns {@code true} if the given key is a Spark-internal property that should be stripped from
   * user-facing table metadata before persisting to the Delta log.
   */
  public static boolean isSparkReservedPropertyKey(String key) {
    switch (key) {
      case TableCatalog.PROP_LOCATION:
      case TableCatalog.PROP_PROVIDER:
      case TableCatalog.PROP_IS_MANAGED_LOCATION:
      case TableCatalog.PROP_COMMENT:
      case TableCatalog.PROP_OWNER:
      case TableCatalog.PROP_EXTERNAL:
      case "path":
      case "option.path":
        return true;
      default:
        return false;
    }
  }
}
