package io.delta.dsv2.table;

import io.delta.dsv2.utils.SchemaUtils;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.util.VectorUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class DeltaTable implements Table, SupportsRead {

  private final String path;
  private DefaultEngine kernelEngine;
  private io.delta.kernel.Table kernelTable;

  public DeltaTable(String path) {
    this.path = path;
  }

  private DefaultEngine getKernelEngine() {
    if (kernelEngine == null) {
      kernelEngine = DefaultEngine.create(new Configuration());
    }
    return kernelEngine;
  }

  private io.delta.kernel.Table getKernelTable() {
    if (kernelTable == null) {
      kernelTable = io.delta.kernel.Table.forPath(getKernelEngine(), path);
    }
    return kernelTable;
  }

  @Override
  public String name() {
    return "delta.`" + path + "`";
  }

  // TODO: this is deprecated product.
  @Override
  public StructType schema() {
    return SchemaUtils.convertKernelSchemaToSparkSchema(
        getKernelTable().getLatestSnapshot(getKernelEngine()).getSchema());
  }

  @Override
  public Set<TableCapability> capabilities() {
    Set<TableCapability> capabilities = new HashSet<>();
    capabilities.add(TableCapability.BATCH_WRITE);
    capabilities.add(TableCapability.BATCH_READ);
    return capabilities;
  }

  @Override
  public Transform[] partitioning() {
    try {
      List<String> partColNames =
          VectorUtils.toJavaList(
              ((SnapshotImpl) getKernelTable().getLatestSnapshot(getKernelEngine()))
                  .getMetadata()
                  .getPartitionColumns());

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
