package io.delta.flink.table;

import java.net.URI;
import java.util.Map;

/**
 * A {@code Catalog} abstracts interaction with an external table catalog or metadata service.
 *
 * <p>The catalog is responsible for resolving logical table identifiers into concrete table
 * metadata and for providing the credentials required to access the underlying storage system. This
 * abstraction allows different catalog implementations (e.g., filesystem-based catalogs,
 * metastore-backed catalogs, or REST-based catalogs) to be used interchangeably by higher-level
 * components.
 *
 * <p>Typical responsibilities of a {@code Catalog} include:
 *
 * <ul>
 *   <li>mapping table identifiers to physical table locations,
 *   <li>providing stable table UUIDs for identification and caching, and
 *   <li>supplying credential or configuration information required for table access.
 * </ul>
 */
public interface Catalog {

  /**
   * Loads metadata for a table identified by the given table identifier.
   *
   * <p>The identifier format and naming conventions are defined by the specific catalog
   * implementation. Implementations may interpret the identifier as a logical name, a
   * fully-qualified path, or another catalog-specific reference.
   *
   * @param tableId the logical identifier of the table to load; must not be {@code null}
   * @return a {@link TableBrief} object describing the resolved table
   * @throws IllegalArgumentException if the identifier is invalid
   * @throws RuntimeException if the table cannot be resolved or loaded
   */
  TableBrief getTable(String tableId);

  /**
   * Returns the credentials or configuration properties required to access the table identified by
   * the given UUID.
   *
   * <p>The returned map may contain authentication information, endpoint configuration, or other
   * filesystem- or catalog-specific properties. The exact contents and semantics are defined by the
   * catalog implementation.
   *
   * @param uuid the unique identifier of the table
   * @return a map of credential or configuration properties; may be empty but never {@code null}
   */
  Map<String, String> getCredentials(String uuid);

  /**
   * A container for table metadata resolved by a {@link Catalog}.
   *
   * <p>{@code TableInfo} describes the essential properties needed to locate and access a table,
   * independent of the underlying catalog implementation.
   */
  class TableBrief {

    /** The logical identifier used to resolve the table. */
    String tableId;

    /** A stable UUID that uniquely identifies the table. */
    String uuid;

    /** The normalized physical location of the table. */
    URI tablePath;
  }
}
