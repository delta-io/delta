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
package io.delta.kernel.spark.utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.immutable.Map$;
import scala.collection.mutable.Builder;
import scala.jdk.javaapi.CollectionConverters;

public final class ScalaUtils {
  public static scala.collection.immutable.Map<String, String> toScalaMap(
      Map<String, String> javaMap) {
    if (javaMap == null) throw new NullPointerException("options");

    // Works on Scala 2.12 and 2.13
    @SuppressWarnings({"rawtypes", "unchecked"})
    Builder<Tuple2<String, String>, scala.collection.immutable.Map<String, String>> b =
        (Builder) Map$.MODULE$.newBuilder();

    for (Map.Entry<String, String> e : javaMap.entrySet()) {
      b.$plus$eq(new Tuple2<>(e.getKey(), e.getValue()));
    }
    return b.result();
  }

  public static Map<String, String> toJavaMap(
      scala.collection.immutable.Map<String, String> scalaMap) {
    if (scalaMap == null) {
      return null;
    }
    if (scalaMap.isEmpty()) {
      return Collections.emptyMap();
    }
    return CollectionConverters.asJava(scalaMap);
  }

  /**
   * Retrieves all Unity Catalog configurations from the SparkSession.
   *
   * <p>This method bridges to Scala code that extracts UC catalog configurations from Spark's
   * catalog manager settings. The Scala implementation is in {@link
   * org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder#getCatalogConfigs}.
   *
   * @param spark the SparkSession containing catalog configurations
   * @return list of UC catalog configurations (catalog name, URI, token)
   * @throws NullPointerException if spark is null
   */
  public static List<UCCatalogConfig> getUCCatalogConfigs(SparkSession spark) {
    if (spark == null) {
      throw new NullPointerException("spark is null");
    }

    // Call Scala code to get catalog configs
    scala.collection.immutable.List<Tuple3<String, String, String>> scalaConfigs =
        org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder$.MODULE$
            .getCatalogConfigs(spark);

    // Convert Scala List<Tuple3> to Java List<UCCatalogConfig>
    return CollectionConverters.asJava(scalaConfigs).stream()
        .map(tuple -> new UCCatalogConfig(tuple._1(), tuple._2(), tuple._3()))
        .collect(Collectors.toList());
  }
}
