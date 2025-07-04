package io.delta.kernel.internal.transaction.builder.context;

import static java.util.Collections.emptyList;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Optional;

public class CreateLikeBuilderContext {
  private final StructType schema;
  private Optional<List<Column>> partitionColumnsOpt = Optional.empty();

  public CreateLikeBuilderContext(StructType schema) {
    this.schema = schema;
  }

  public void withPartitionColumns(List<Column> partitionColumns) {
    this.partitionColumnsOpt = Optional.ofNullable(partitionColumns);
  }

  public StructType getSchema() {
    return schema;
  }

  public Optional<List<Column>> getPartitionColumnsOpt() {
    return partitionColumnsOpt;
  }

  public Protocol getInitialProtocol() {
    return null;
  }

  public Metadata getInitialMetadata() {
    return null;
  }

  public void validateCreateLikeInputs() {
    validateNotPartitionColsAndClusterCols();
    validateSchema();
    validatePartitionColumns();
  }

  public void validateNotPartitionColsAndClusterCols() {}

  public void validateSchema() {
    // TODO: Column mapping mode should be passed from table properties
    SchemaUtils.validateSchema(schema, false /* columnMappingEnabled */);
  }

  public void validatePartitionColumns() {
    SchemaUtils.validatePartitionColumns(schema, partitionColumnsOpt.orElse(emptyList()));
  }


}
