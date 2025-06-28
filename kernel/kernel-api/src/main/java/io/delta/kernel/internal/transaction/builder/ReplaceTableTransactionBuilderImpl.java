package io.delta.kernel.internal.transaction.builder;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.table.ResolvedTableInternal;
import io.delta.kernel.internal.transaction.TransactionFactory;
import io.delta.kernel.transaction.ReplaceTableTransactionBuilder;
import io.delta.kernel.transaction.Transaction;
import io.delta.kernel.types.StructType;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ReplaceTableTransactionBuilderImpl extends CreateLikeTransactionBuilder
    implements ReplaceTableTransactionBuilder {

  private final ResolvedTableInternal baseTable;

  private static final Set<String> TABLE_PROPERTY_KEYS_TO_PRESERVE =
      new HashSet<String>() {
        {
          add(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey());
        }
      };

  public ReplaceTableTransactionBuilderImpl(ResolvedTableInternal baseTable, StructType newSchema) {
    super(newSchema);
    this.baseTable = baseTable;
  }

  @Override
  protected ReplaceTableTransactionBuilderImpl self() {
    return this;
  }

  @Override
  public Transaction build(Engine engine) {
    return TransactionFactory.createTransaction(
        baseTable.getPath(),
        baseTable.getVersion(),
        baseTable.getTimestamp(),
        baseTable.getProtocol(),
        getStartMetadata()
    );
  }

  private Metadata getStartMetadata() {
    final Map<String, String> propertiesToPreserve =
        baseTable.getMetadata().getConfiguration().entrySet().stream()
            .filter(
                e ->
                    TABLE_PROPERTY_KEYS_TO_PRESERVE.contains(
                        e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return getInitialMetadata().withMergedConfiguration(propertiesToPreserve);
  }
}
