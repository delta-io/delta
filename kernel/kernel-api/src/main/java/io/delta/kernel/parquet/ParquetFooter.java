package io.delta.kernel.parquet;

import io.delta.kernel.types.StructType;

/**
 * Class representing a subset of information from the Parquet file footer.
 */
public class ParquetFooter
{
    /**
     * Return the schema of the file from the Parquet footer.
     * @return Parquet file physical schema.
     */
    StructType getSchema() {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
