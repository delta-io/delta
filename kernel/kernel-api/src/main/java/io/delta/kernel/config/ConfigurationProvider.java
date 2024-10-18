package io.delta.kernel.config;

import java.util.Optional;

public interface ConfigurationProvider {
  String get(String key);

  Optional<String> getOptional(String key);

  boolean contains(String key);
}
