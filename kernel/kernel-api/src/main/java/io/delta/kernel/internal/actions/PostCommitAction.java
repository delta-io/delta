package io.delta.kernel.internal.actions;

import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import java.io.IOException;

public interface PostCommitAction {

  void threadSafeInvoke(Table table, Engine engine) throws IOException;

  String getType();
}
