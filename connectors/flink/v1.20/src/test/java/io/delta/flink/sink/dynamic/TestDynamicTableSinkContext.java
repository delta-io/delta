package io.delta.flink.sink.dynamic;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.factories.DynamicTableFactory;

public class TestDynamicTableSinkContext implements DynamicTableFactory.Context {

  private final ResolvedCatalogTable table;

  public TestDynamicTableSinkContext(ResolvedCatalogTable table) {
    this.table = table;
  }

  @Override
  public ObjectIdentifier getObjectIdentifier() {
    return ObjectIdentifier.of("default", "default", "t");
  }

  @Override
  public ResolvedCatalogTable getCatalogTable() {
    return table;
  }

  @Override
  public Map<String, String> getEnrichmentOptions() {
    return Map.of();
  }

  @Override
  public Configuration getConfiguration() {
    return new Configuration();
  }

  @Override
  public ClassLoader getClassLoader() {
    return Thread.currentThread().getContextClassLoader();
  }

  @Override
  public boolean isTemporary() {
    return true;
  }
}
