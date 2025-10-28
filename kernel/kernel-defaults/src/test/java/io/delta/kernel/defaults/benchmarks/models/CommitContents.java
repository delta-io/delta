package io.delta.kernel.defaults.benchmarks.models;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.IOException;
import java.util.List;

public class CommitContents {
  List<DataFileStatus> add_actions;

  public CommitContents(List<DataFileStatus> add_actions) throws IOException {
    this.add_actions = add_actions;
  }

  public CloseableIterator<DataFileStatus> getAdd_actions() {
    return toCloseableIterator(add_actions.iterator());
  }
}
