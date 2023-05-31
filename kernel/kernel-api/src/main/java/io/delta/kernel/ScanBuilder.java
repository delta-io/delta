package io.delta.kernel;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.StructType;

/**
 * Builder to construct {@link Scan} object.
 */
public interface ScanBuilder {

    /**
     * Apply the given filter expression to prune any files
     * that do not contain data satisfying the given filter.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param filter an {@link Expression} which evaluates to boolean.
     * @return A {@link ScanBuilder} with filter applied.
     *
     * @throws InvalidExpressionException if the filter is not valid.
     */
    ScanBuilder withFilter(TableClient tableClient, Expression filter)
            throws InvalidExpressionException;

    /**
     * Apply the given <i>readSchema</i>.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param readSchema Subset of columns to read from the Delta table.
     * @return A {@link ScanBuilder} with projection pruning.
     */
    ScanBuilder withReadSchema(TableClient tableClient, StructType readSchema);

    /**
     * @return Build the {@link Scan instance}
     */
    Scan build();
}
