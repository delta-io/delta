package io.delta.kernel;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

public interface PaginatedScan extends Scan {

  @Override
  CloseableIterator<FilteredColumnarBatch> getScanFiles(Engine engine);

  Row getNewPageToken();

  ColumnarBatch getTombStoneHashsets();
}
