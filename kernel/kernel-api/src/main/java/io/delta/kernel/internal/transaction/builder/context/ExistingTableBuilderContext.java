package io.delta.kernel.internal.transaction.builder.context;

import io.delta.kernel.internal.table.ResolvedTableInternal;
import io.delta.kernel.internal.tablefeatures.TableFeatures;

public class ExistingTableBuilderContext {
  private ResolvedTableInternal baseTable;

  public ExistingTableBuilderContext(ResolvedTableInternal baseTable) {
    this.baseTable = baseTable;
  }

  public ResolvedTableInternal getBaseTable() {
    return baseTable;
  }

  public void validateKernelCanWriteToTable() {
    TableFeatures.validateKernelCanWriteToTable(
        baseTable.getProtocol(), baseTable.getMetadata(), baseTable.getPath());
  }
}
