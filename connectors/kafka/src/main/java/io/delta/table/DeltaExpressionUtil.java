/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta.table;

import io.delta.kernel.expressions.AlwaysFalse;
import io.delta.kernel.expressions.AlwaysTrue;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Or;
import io.delta.kernel.expressions.Predicate;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

public class DeltaExpressionUtil {
  private DeltaExpressionUtil() {}

  private static final String IS_NULL = "IS_NULL";
  private static final String NOT_NULL = "IS_NOT_NULL";
  private static final String NOT = "NOT";
  private static final String EQUAL = "=";
  private static final String LESS_THAN = "<";
  private static final String LESS_THAN_OR_EQUAL = "<=";
  private static final String GREATER_THAN = ">";
  private static final String GREATER_THAN_OR_EQUAL = ">=";

  public static Predicate convert(org.apache.iceberg.expressions.Expression expr) {
    return ExpressionVisitors.visit(expr, ToDeltaExpression.INSTANCE);
  }

  private static class ToDeltaExpression
      extends ExpressionVisitors.BoundExpressionVisitor<Predicate> {
    private static final ToDeltaExpression INSTANCE = new ToDeltaExpression();

    private ToDeltaExpression() {}

    @Override
    public <T> Predicate isNull(BoundReference<T> ref) {
      return new Predicate(IS_NULL, column(ref));
    }

    @Override
    public <T> Predicate notNull(BoundReference<T> ref) {
      return new Predicate(NOT_NULL, column(ref));
    }

    @Override
    public <T> Predicate isNaN(BoundReference<T> ref) {
      throw new UnsupportedOperationException("Cannot convert isNaN expression");
    }

    @Override
    public <T> Predicate notNaN(BoundReference<T> ref) {
      throw new UnsupportedOperationException("Cannot convert notNaN expression");
    }

    @Override
    public <T> Predicate lt(BoundReference<T> ref, Literal<T> lit) {
      return new Predicate(LESS_THAN, column(ref), literal(lit.value(), ref.type()));
    }

    @Override
    public <T> Predicate ltEq(BoundReference<T> ref, Literal<T> lit) {
      return new Predicate(LESS_THAN_OR_EQUAL, column(ref), literal(lit.value(), ref.type()));
    }

    @Override
    public <T> Predicate gt(BoundReference<T> ref, Literal<T> lit) {
      return new Predicate(GREATER_THAN, column(ref), literal(lit.value(), ref.type()));
    }

    @Override
    public <T> Predicate gtEq(BoundReference<T> ref, Literal<T> lit) {
      return new Predicate(GREATER_THAN_OR_EQUAL, column(ref), literal(lit.value(), ref.type()));
    }

    @Override
    public <T> Predicate eq(BoundReference<T> ref, Literal<T> lit) {
      return new Predicate(EQUAL, column(ref), literal(lit.value(), ref.type()));
    }

    @Override
    public <T> Predicate notEq(BoundReference<T> ref, Literal<T> lit) {
      return not(eq(ref, lit));
    }

    @Override
    public <T> Predicate in(BoundReference<T> ref, Set<T> literalSet) {
      throw new UnsupportedOperationException("Cannot convert IN expression");
    }

    @Override
    public <T> Predicate notIn(BoundReference<T> ref, Set<T> literalSet) {
      throw new UnsupportedOperationException("Cannot convert NOT IN expression");
    }

    @Override
    public <T> Predicate startsWith(BoundReference<T> ref, Literal<T> lit) {
      throw new UnsupportedOperationException("Cannot convert startsWith expression");
    }

    @Override
    public <T> Predicate notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      throw new UnsupportedOperationException("Cannot convert notStartsWith expression");
    }

    @Override
    public Predicate alwaysTrue() {
      return AlwaysTrue.ALWAYS_TRUE;
    }

    @Override
    public Predicate alwaysFalse() {
      return AlwaysFalse.ALWAYS_FALSE;
    }

    @Override
    public Predicate not(Predicate result) {
      return new Predicate(NOT, result);
    }

    @Override
    public Predicate and(Predicate leftResult, Predicate rightResult) {
      return new And(leftResult, rightResult);
    }

    @Override
    public Predicate or(Predicate leftResult, Predicate rightResult) {
      return new Or(leftResult, rightResult);
    }
  }

  public static <T> io.delta.kernel.expressions.Literal literal(T value, Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return io.delta.kernel.expressions.Literal.ofBoolean((Boolean) value);
      case INTEGER:
        return io.delta.kernel.expressions.Literal.ofInt((Integer) value);
      case LONG:
        return io.delta.kernel.expressions.Literal.ofLong((Long) value);
      case FLOAT:
        return io.delta.kernel.expressions.Literal.ofFloat((Float) value);
      case DOUBLE:
        return io.delta.kernel.expressions.Literal.ofDouble((Double) value);
      case DATE:
        return io.delta.kernel.expressions.Literal.ofDate((Integer) value);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return io.delta.kernel.expressions.Literal.ofTimestamp((Long) value);
        } else {
          return io.delta.kernel.expressions.Literal.ofTimestampNtz((Long) value);
        }
      case STRING:
        return io.delta.kernel.expressions.Literal.ofString((String) value);
      case UUID:
        return io.delta.kernel.expressions.Literal.ofString(value.toString());
      case FIXED:
      case BINARY:
        return io.delta.kernel.expressions.Literal.ofBinary(
            ByteBuffers.toByteArray((ByteBuffer) value));
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) type;
        return io.delta.kernel.expressions.Literal.ofDecimal(
            (BigDecimal) value, decimal.precision(), decimal.scale());
    }

    throw new UnsupportedOperationException(
        "Cannot convert literal " + value + " for type " + type);
  }

  public static Column column(BoundReference<?> ref) {
    return new Column(ref.name());
  }
}
