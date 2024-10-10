package io.delta.kafka;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.connect.IcebergSinkConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

public class DeltaSinkConnector extends IcebergSinkConnector {

  @Override
  public String version() {
    return DeltaSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> connectorProps) {
    super.start(connectorProps);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return super.taskClass();
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return super.taskConfigs(maxTasks);
  }

  @Override
  public void stop() {
    super.stop();
  }

  @Override
  public ConfigDef config() {
    return super.config();
  }
}
