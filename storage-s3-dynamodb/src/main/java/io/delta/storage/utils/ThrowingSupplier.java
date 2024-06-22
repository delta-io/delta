package io.delta.storage.utils;

@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;
}
