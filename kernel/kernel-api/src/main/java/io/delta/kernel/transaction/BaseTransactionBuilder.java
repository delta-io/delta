package io.delta.kernel.transaction;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import java.util.List;
import java.util.Map;

public interface BaseTransactionBuilder<T extends BaseTransactionBuilder<T>> {
  T withTableProperties(Map<String, String> properties);

  // TODO: where to put SetTransaction?

  T withClusteringColumns(List<Column> clusteringColumns);

  Transaction build(Engine engine);
}
