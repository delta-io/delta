package io.delta.dsv2.catalog;

import io.delta.dsv2.table.DeltaCcv2Table;
import io.delta.dsv2.table.DeltaTable;
import io.delta.dsv2.utils.SchemaUtils;
import io.delta.kernel.Operation;
import io.delta.kernel.ResolvedTable;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class TestCatalog implements TableCatalog {

  private String catalogName;
  private DefaultEngine engine;

  public String tableIdentifierToPath(Identifier ident) {
    if (ident.namespace().length != 1 || !ident.namespace()[0].equals("delta")) {
      throw new IllegalArgumentException("Table identifier must have namespace ['delta']");
    }
    return ident.name();
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.engine = DefaultEngine.create(new Configuration());
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    return new Identifier[0];
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      Engine engine = DefaultEngine.create(new Configuration());
      ResolvedTable table = TableManager.loadTable(tableIdentifierToPath(ident)).build(engine);
      return new DeltaCcv2Table(table, ident, engine, "", "", "");
    } catch (TableNotFoundException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    String path = tableIdentifierToPath(ident);

    if (partitions.length > 0) {
      throw new UnsupportedOperationException("partition table is not supported");
    }

    Map<String, String> deltaProperties =
        properties.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith("delta."))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    io.delta.kernel.Table.forPath(engine, path)
        .createTransactionBuilder(engine, "kernel-spark-dsv2", Operation.CREATE_TABLE)
        .withSchema(engine, SchemaUtils.convertSparkSchemaToKernelSchema(schema))
        .withPartitionColumns(engine, new ArrayList<String>())
        .withTableProperties(engine, deltaProperties)
        .build(engine)
        .commit(engine, io.delta.kernel.utils.CloseableIterable.emptyIterable());

    return new DeltaTable(path);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return true;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("renameTable is not supported");
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException("alterTable is not supported");
  }

  @Override
  public boolean tableExists(Identifier ident) {
    try {
      DeltaTable table = new DeltaTable(tableIdentifierToPath(ident));
      table.schema(); // Validate table exists by accessing schema
      return true;
    } catch (TableNotFoundException e) {
      return false;
    }
  }

  @Override
  public String name() {
    return catalogName;
  }
}
