/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.benchmarks;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.delta.kernel.defaults.utils.ExpressionUtils;
import io.delta.kernel.expressions.Predicate;
import java.io.IOException;

/** Custom Jackson deserializer that converts SQL predicate strings to Delta Kernel predicates. */
public class PredicateDeserializer extends JsonDeserializer<Predicate> {

  @Override
  public Predicate deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    String sqlPredicate = p.getValueAsString();

    try {
      return ExpressionUtils.parseSqlPredicate(sqlPredicate);
    } catch (Exception e) {
      throw new IOException("Failed to parse SQL predicate: " + sqlPredicate, e);
    }
  }
}
