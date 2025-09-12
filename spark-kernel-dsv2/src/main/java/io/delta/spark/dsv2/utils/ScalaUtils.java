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
import java.util.Objects;
import scala.Predef;
import scala.collection.JavaConverters;

/** A utility class for converting between Java and Scala types. */
public class ScalaUtils {
  public static scala.collection.immutable.Map<String, String> toScalaMap(
      Map<String, String> javaMap) {
    return JavaConverters.mapAsScalaMapConverter(Objects.requireNonNull(javaMap, "options"))
        .asScala()
        .toMap(Predef.conforms());
  }
}
