package io.delta.kafka;

import java.util.Map;
import org.apache.iceberg.connect.IcebergSinkConfig;

public class DeltaSinkConfig extends IcebergSinkConfig {
  public DeltaSinkConfig(Map<String, String> originalProps) {
    super(originalProps);
  }

  public static String version() {
    return DeltaKafkaMeta.DELTA_KAFKA_CONNECT_VERSION;
  }
}
