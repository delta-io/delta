package io.delta.catalog;

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class UnityCatalog implements Catalog {
  @Override
  public String name() {
    return "unity-catalog";
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
