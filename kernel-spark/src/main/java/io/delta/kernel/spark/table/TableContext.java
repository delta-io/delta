package io.delta.kernel.spark.table;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.spark.unity.UnityCatalogClientFactory;
import java.util.Optional;

/**
 * Encapsulates optional metadata associated with a {@link SparkTable}.
 *
 * <p>The context is intentionally lightweight; it currently carries Unity Catalog client state when
 * available and can be extended with additional table-specific wiring over time.
 */
public final class TableContext {

  private final Optional<UnityCatalogClientFactory.UnityCatalogClient> unityCatalogClient;

  private TableContext(Optional<UnityCatalogClientFactory.UnityCatalogClient> unityCatalogClient) {
    this.unityCatalogClient =
        requireNonNull(unityCatalogClient, "unityCatalogClient optional must be non-null");
  }

  public static TableContext empty() {
    return new TableContext(Optional.empty());
  }

  public static TableContext withUnityCatalogClient(
      UnityCatalogClientFactory.UnityCatalogClient unityCatalogClient) {
    requireNonNull(unityCatalogClient, "unityCatalogClient is null");
    return new TableContext(Optional.of(unityCatalogClient));
  }

  public Optional<UnityCatalogClientFactory.UnityCatalogClient> unityCatalogClient() {
    return unityCatalogClient;
  }
}
