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

package org.apache.spark.sql.delta.catalog;

/**
 * A Spark catalog plugin for Delta Lake tables that implements the Spark DataSource V2 Catalog API.
 *
 * To use this catalog, configure it in your Spark session:
 * <pre>{@code
 * // Scala example
 * val spark = SparkSession
 *   .builder()
 *   .appName("...")
 *   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
 *   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
 *   .getOrCreate()
 *
 *
 * // Python example
 * spark = SparkSession \
 *   .builder \
 *   .appName("...") \
 *   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
 *   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
 *   .getOrCreate()
 * }</pre>
 *
 * <h2>Architecture and Delegation Logic</h2>
 *
 * This class sits in the delta-spark (unified) module and provides a single entry point for
 * Delta Lake catalog operations.
 *
 * <p>The unified module can access both implementations:</p>
 * <ul>
 *   <li>V1 (hybrid DSv1/DSv2): org.apache.spark.sql.delta.catalog.DeltaTableV2 - Connector using DeltaLog</li>
 *   <li>V2 (Pure DSv2): io.delta.kernel.spark.SparkTable - Kernel-backed connector</li>
 * </ul>
 *
 * @see AbstractDeltaCatalog for the base implementation
 * @see io.delta.sql.DeltaSparkSessionExtension for Delta SQL extensions
 */
public class DeltaCatalog extends AbstractDeltaCatalog {
}
