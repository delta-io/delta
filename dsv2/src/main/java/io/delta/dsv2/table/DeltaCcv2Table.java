package io.delta.dsv2.table;

import io.delta.dsv2.utils.SchemaUtils;
import io.delta.kernel.ResolvedTable;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.exceptions.TableNotFoundException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class DeltaCcv2Table implements Table, SupportsRead {

  private final ResolvedTable resolvedTable;
  private final Identifier tableIdentifier;
  private DefaultEngine kernelEngine;

  public DeltaCcv2Table(ResolvedTable resolvedTable, Identifier tableIdentifier) {
    this.resolvedTable = resolvedTable;
    this.tableIdentifier = tableIdentifier;
  }

  @Override
  public String name() {
    return tableIdentifier.name();
  }

  // TODO: this is deprecated product.
  @Override
  public StructType schema() {
    return SchemaUtils.convertKernelSchemaToSparkSchema(resolvedTable.getSchema());
  }

  @Override
  public Set<TableCapability> capabilities() {
    Set<TableCapability> capabilities = new HashSet<>();
    capabilities.add(TableCapability.BATCH_READ);
    return capabilities;
  }

  @Override
  public Transform[] partitioning() {
    try {
      List<String> partColNames =
          resolvedTable.getPartitionColumns().stream()
              .map(column -> String.join(".", column.getNames()))
              .collect(Collectors.toList());

      Transform[] transforms = new Transform[partColNames.size()];
      for (int i = 0; i < partColNames.size(); i++) {
        transforms[i] = Expressions.identity(partColNames.get(i));
      }
      return transforms;
    } catch (TableNotFoundException e) {
      return new Transform[0];
    }
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    throw new UnsupportedOperationException("todo: fix the scan");
  }
}
