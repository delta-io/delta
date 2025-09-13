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
package io.delta.spark.dsv2.utils;

import java.util.Map;
import scala.Tuple2;
import scala.collection.immutable.Map$;
import scala.collection.mutable.Builder;

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
}
