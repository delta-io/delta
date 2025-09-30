package io.delta.kernel.spark.exceptions;

import io.delta.kernel.exceptions.KernelException;

/**
 * Thrown when a specified column does not exist in schema; cannot create a BoundReference for a
 * NamedReference.
 */
public class NonexistentColumnException extends KernelException {

  private static final String message = "The column '%s' does not exist.";

  public NonexistentColumnException(String columnName) {
    super(String.format(message, columnName));
  }
}
