package io.delta.kernel;

/** Identifier for a table. e.g. <catalog> / <schema> / <tableName> */
public class TableIdentifier {
  /** The name of the table. */
  private final String name;

  /** The namespace of the table. e.g. <catalog> / <schema> */
  private final String[] namespace;

  public TableIdentifier(String[] namespace, String name) {
    this.namespace = namespace;
    this.name = name;
  }

  /** Returns the namespace of the table. */
  public String[] getNamespace() {
    return namespace;
  }

  /** Returns the name of the table. */
  public String getName() {
    return name;
  }
}
