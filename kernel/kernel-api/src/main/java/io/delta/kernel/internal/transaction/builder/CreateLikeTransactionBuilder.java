package io.delta.kernel.internal.transaction.builder;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
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

  /////////////////////////////////////////
  // CreateTableTransactionBuilder methods //
  /////////////////////////////////////////

//  @Override
//  public CreateTableTransactionBuilder withSchema(StructType schema) {
//    this.schemaOpt = Optional.of(schema);
//    return this;
//  }

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


}
