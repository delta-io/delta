package io.delta.flink.table;

import io.delta.kernel.types.StructType;

import java.net.URI;
import java.util.List;

/**
 *
 */
public class LocalKernelTable extends AbstractKernelTable {

    public LocalKernelTable(URI tablePath) {
        super(tablePath);
    }

    public LocalKernelTable(URI tablePath, StructType schema, List<String> partitionColumns) {
       super(tablePath, schema, partitionColumns);
    }

    @Override
    public String getId() {
        return getTablePath().toString();
    }
}
