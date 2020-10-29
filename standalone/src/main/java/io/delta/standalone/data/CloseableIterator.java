package io.delta.standalone.data;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An iterator that must be closed.
 *
 * @param <T>  element type
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable { }
