package io.delta.kernel.internal.transaction.builder;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.transaction.BaseTransactionBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class BaseTransactionBuilderImpl<T extends BaseTransactionBuilder<T>>
    implements BaseTransactionBuilder<T> {

  protected final long currentTimeMillis = System.currentTimeMillis();
  protected Optional<Map<String, String>> newTablePropertiesOpt;
  private Optional<List<Column>> clusteringColumnsOpt = Optional.empty();
  protected Optional<SetTransaction> setTxnOpt = Optional.empty();

  @Override
  public T withTableProperties(Map<String, String> properties) {
    requireNonNull(properties, "properties is null");
    if (!properties.isEmpty()) {
      this.newTablePropertiesOpt = Optional.of(properties);
    }
    return self();
  }

  @Override
  public T withClusteringColumns(List<Column> clusteringColumns) {
    this.clusteringColumnsOpt = Optional.of(clusteringColumns);
    return self();
  }

  protected abstract T self();
}
