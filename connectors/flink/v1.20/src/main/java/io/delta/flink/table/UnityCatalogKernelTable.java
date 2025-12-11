package io.delta.flink.table;

import java.net.URI;

public class UnityCatalogKernelTable extends AbstractKernelTable {

    public UnityCatalogKernelTable() {
        super(null);
    }

    @Override
    public String getId() {
        return "";
    }
}
