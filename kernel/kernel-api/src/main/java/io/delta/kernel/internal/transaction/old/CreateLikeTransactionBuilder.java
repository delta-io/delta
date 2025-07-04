package io.delta.kernel.internal.transaction.old;

import static io.delta.kernel.internal.util.ColumnMapping.isColumnMappingModeEnabled;
import static java.util.Collections.emptyList;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.transaction.builder.BaseTransactionBuilderImpl;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Optional;

public abstract class CreateLikeTransactionBuilder
    extends BaseTransactionBuilderImpl<CreateTableTransactionBuilder>
    implements CreateTableTransactionBuilder {

  private final StructType schema;
  protected Optional<List<Column>> partitionColumnsOpt = Optional.empty();
  protected boolean domainMetadataSupported = false;

  protected CreateLikeTransactionBuilder(StructType schema) {
    this.schema = schema;
  }

  @Override
  public CreateTableTransactionBuilder withPartitionColumns(List<Column> partitionColumns) {
    this.partitionColumnsOpt = Optional.of(partitionColumns);
    return this;
  }

  protected Protocol getInitialProtocol() {
    return null;
  }

  protected Metadata getInitialMetadata() {
    return null;
  }

  protected void validateCreateLikeInputs() {
    validateNotPartitionColsAndClusterCols();
    SchemaUtils.validateSchema(schema, isColumnMappingModeEnabled(mappingMode));
    SchemaUtils.validatePartitionColumns(schema, partitionColumnsOpt.orElse(emptyList()));
  }

  protected void validateNotPartitionColsAndClusterCols() {}
}
