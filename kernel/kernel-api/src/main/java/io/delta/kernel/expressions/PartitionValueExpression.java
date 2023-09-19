/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import java.util.Collections;
import java.util.List;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.DataType;

/**
 * Expression to decode the serialized partition value into partition type value according the
 * <a href=https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization>
 * Delta Protocol spec</a>.
 * <p>
 * <ul>
 * <li>Name: <code>partition_value</code>
 * <li>Semantic: <code>partition_value(string, datatype)</code>. Decode the partition
 * value of type <i>datatype</i> from the serialized string format.</li>
 * </ul>
 *
 * @since 3.0.0
 */
@Evolving
public class PartitionValueExpression implements Expression {
    private final DataType partitionValueType;
    private final Expression serializedPartitionValue;

    /**
     * Create {@code partition_value} expression.
     *
     * @param serializedPartitionValue Input expression providing the partition values in
     *                                 serialized format.
     * @param partitionDataType        Partition data type to which string partition value is
     *                                 deserialized as according to the Delta Protocol.
     */
    public PartitionValueExpression(
        Expression serializedPartitionValue, DataType partitionDataType) {
        this.serializedPartitionValue = requireNonNull(serializedPartitionValue);
        this.partitionValueType = requireNonNull(partitionDataType);
    }

    /**
     * Get the expression reference to the serialized partition value.
     */
    public Expression getInput() {
        return serializedPartitionValue;
    }

    /**
     * Get the data type of the partition value.
     */
    public DataType getDataType() {
        return partitionValueType;
    }

    @Override
    public List<Expression> getChildren() {
        return Collections.singletonList(serializedPartitionValue);
    }

    @Override
    public String toString() {
        return format("partition_value(%s, %s)", serializedPartitionValue, partitionValueType);
    }
}
