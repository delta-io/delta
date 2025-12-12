package io.delta.flink.table;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** An implementation of KernelTable that access tables in file system. */
public class FileSystemKernelTable extends AbstractKernelTable {

  public FileSystemKernelTable(URI tablePath) {
    super(Map.of());
    Objects.requireNonNull(tablePath);
    this.tablePath = normalize(tablePath);
    postInit();
  }

  public FileSystemKernelTable(URI tablePath, Map<String, String> conf) {
    super(conf);
    Objects.requireNonNull(tablePath);
    this.tablePath = normalize(tablePath);
    postInit();
  }

  public FileSystemKernelTable(URI tablePath, StructType schema, List<String> partitionColumns) {
    super(Map.of(), schema, partitionColumns);
    Objects.requireNonNull(tablePath);
    this.tablePath = normalize(tablePath);
    postInit();
  }

  public FileSystemKernelTable(
      URI tablePath, Map<String, String> conf, StructType schema, List<String> partitionColumns) {
    super(conf, schema, partitionColumns);
    Objects.requireNonNull(tablePath);
    this.tablePath = normalize(tablePath);
    postInit();
  }

  @Override
  public String getId() {
    return getTablePath().toString();
  }

  @Override
  protected Snapshot loadLatestSnapshot() {
    return TableManager.loadSnapshot(getTablePath().toString()).build(getEngine());
  }
}
