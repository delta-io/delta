package io.delta.kernel.internal.transaction.builder;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.table.ResolvedTableInternal;
import io.delta.kernel.internal.transaction.TransactionFactory;
import io.delta.kernel.internal.transaction.builder.context.CreateLikeBuilderContext;
import io.delta.kernel.internal.transaction.builder.context.ExistingTableBuilderContext;
import io.delta.kernel.transaction.ReplaceTableTransactionBuilder;
import io.delta.kernel.transaction.Transaction;
import io.delta.kernel.types.StructType;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ReplaceTableTransactionBuilderImpl
    extends BaseTransactionBuilderImpl<ReplaceTableTransactionBuilder>
    implements ReplaceTableTransactionBuilder {

  private static final Set<String> TABLE_PROPERTY_KEYS_TO_PRESERVE =
      new HashSet<String>() {
        {
          add(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey());
        }
      };

  private final CreateLikeBuilderContext createContext;
  private final ExistingTableBuilderContext existingContext;

  public ReplaceTableTransactionBuilderImpl(ResolvedTableInternal baseTable, StructType newSchema) {
    super();
    this.createContext = new CreateLikeBuilderContext(newSchema);
    this.existingContext = new ExistingTableBuilderContext(baseTable);
  }

  @Override
  public ReplaceTableTransactionBuilderImpl withPartitionColumns(List<Column> partitionColumns) {
    createContext.withPartitionColumns(partitionColumns);
    return this;
  }

  @Override
  protected ReplaceTableTransactionBuilderImpl self() {
    return this;
  }

  @Override
  public Transaction build(Engine engine) {
    existingContext.validateKernelCanWriteToTable();
    createContext.validateCreateLikeInputs();

    return TransactionFactory.createTransaction(
        existingContext.getBaseTable().getPath(),
        existingContext.getBaseTable().getVersion(),
        existingContext.getBaseTable().getTimestamp(),
        existingContext.getBaseTable().getProtocol(),
        getStartMetadata());
  }

  private Metadata getStartMetadata() {
    final Map<String, String> propertiesToPreserve =
        existingContext.getBaseTable().getMetadata().getConfiguration().entrySet().stream()
            .filter(e -> TABLE_PROPERTY_KEYS_TO_PRESERVE.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return createContext.getInitialMetadata().withMergedConfiguration(propertiesToPreserve);
  }
}
