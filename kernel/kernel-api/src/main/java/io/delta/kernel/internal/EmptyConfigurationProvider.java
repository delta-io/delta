package io.delta.kernel.internal;

import io.delta.kernel.config.ConfigurationProvider;
import java.util.NoSuchElementException;
import java.util.Optional;

public class EmptyConfigurationProvider implements ConfigurationProvider {
  @Override
  public String get(String key) {
    throw new NoSuchElementException();
  }

  @Override
  public Optional<String> getOptional(String key) {
    return Optional.empty();
  }

  @Override
  public boolean contains(String key) {
    return false;
  }
}
