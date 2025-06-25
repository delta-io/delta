package io.delta.kernel.transaction;

import io.delta.kernel.engine.Engine;
import java.util.Map;

public interface BaseTransactionBuilder<T extends BaseTransactionBuilder<T>> {
  T withTableProperties(Map<String, String> properties);

  T withTransactionId(String applicationId, long transactionVersion);

  Transaction build(Engine engine);
}
