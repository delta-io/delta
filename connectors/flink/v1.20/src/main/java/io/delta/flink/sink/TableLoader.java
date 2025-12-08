package io.delta.flink.sink;

import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;

public interface TableLoader extends Serializable {

  default Engine getEngine() {
    return DefaultEngine.create(new Configuration());
  }

  Table loadTable();

  class PathBasedTableLoader implements TableLoader {

    private final String tablePath;

    public PathBasedTableLoader(String tablePath) {
      this.tablePath = tablePath;
    }

    @Override
    public Table loadTable() {
      return Table.forPath(getEngine(), tablePath);
    }
  }
}
