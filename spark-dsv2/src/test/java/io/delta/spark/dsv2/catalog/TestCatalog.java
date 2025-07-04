package io.delta.spark.dsv2.catalog;

import java.util.*;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class TestCatalog implements TableCatalog {

  private String catalogName;

  @Override
  public Identifier[] listTables(String[] namespace) {
    throw new UnsupportedOperationException("listTables method is not implemented");
  }

  @Override
  public Table loadTable(Identifier ident) {
    throw new UnsupportedOperationException("loadTable method is not implemented");
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    throw new UnsupportedOperationException("createTable method is not implemented");
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException("alterTable method is not implemented");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    throw new UnsupportedOperationException("dropTable method is not implemented");
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("renameTable method is not implemented");
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }
}
