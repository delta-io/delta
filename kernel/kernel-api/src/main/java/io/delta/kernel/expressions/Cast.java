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
package io.delta.kernel.expressions;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.DataType;
import java.util.Collections;
import java.util.List;

/**
 * A cast expression to convert the input type to another given type.
 *
 * @since 3.3.0
 */
@Evolving
public final class Cast implements Expression {
  private final Expression child;
  private final DataType outputType;

  /** Create a cast around the given input expression to specified output data type. */
  public Cast(Expression child, DataType outputType) {
    this.child = child;
    this.outputType = outputType;
  }

  /** @return the target data type of this cast expression. */
  public DataType getOutputType() {
    return outputType;
  }

  @Override
  public List<Expression> getChildren() {
    return Collections.singletonList(child);
  }

  @Override
  public String toString() {
    return String.format("CAST(%s AS %s)", child, outputType);
  }
}
