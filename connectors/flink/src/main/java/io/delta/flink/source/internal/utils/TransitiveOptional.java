package io.delta.flink.source.internal.utils;


import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A container object which may or may not contain a non-null value. It supports chaining through
 *
 * <p>
 * <p>
 * <p>
 * An example of using {@code TransitiveOptional} class for method chaining where every method
 * returns {@code TransitiveOptional} instance. In this chain, the next step will be executed ONLY
 * if the previous one returned an empty {@code TransitiveOptional}
 * <pre>
 *     return getSnapshotFromCheckpoint(checkpointSnapshotVersion)
 *            .or(this::getSnapshotFromStartingVersionOption)
 *            .or(this::getSnapshotFromStartingTimestampOption)
 *            .or(this::getHeadSnapshot)
 *            .get();
 * </pre>
 *
 * @param <T> the type of value
 * @apiNote This class is a simplified version of Java's {@link java.util.Optional} class. This
 * class does NOT support all Java's {@code Optional} methods and its goal is NOT to replace Java's
 * {@code Optional}. Its main and only purpose is to use it whenever you need an Optional chaining
 * which was introduced in Java 9+
 */
public class TransitiveOptional<T> {

    /**
     * Common instance for {@code empty()}.
     */
    private static final TransitiveOptional<?> EMPTY = new TransitiveOptional<>();

    /**
     * If non-null, the value; if null, indicates no value is present
     */
    private final T value;

    /**
     * Constructs an empty instance.
     *
     * @implNote Generally only one empty instance, {@link TransitiveOptional#EMPTY}, should exist
     * per VM.
     */
    private TransitiveOptional() {
        this.value = null;
    }

    /**
     * Constructs an instance with the described value.
     *
     * @param value the non-{@code null} value to describe
     * @throws NullPointerException if value is {@code null}
     */
    private TransitiveOptional(T value) {
        Objects.requireNonNull(value);
        this.value = value;
    }

    /**
     * Returns an {@code TransitiveOptional} describing the given non-{@code null} value.
     *
     * @param value the value to describe, which must be non-{@code null}
     * @param <T>   the type of the value
     * @return an {@code TransitiveOptional} with the value present
     * @throws NullPointerException if value is {@code null}
     */
    public static <T> TransitiveOptional<T> of(T value) {
        return new TransitiveOptional<>(value);
    }

    /**
     * Returns an {@code TransitiveOptional} describing the given value, if non-{@code null},
     * otherwise returns an empty {@code TransitiveOptional}.
     *
     * @param value the possibly-{@code null} value to describe
     * @param <T>   the type of the value
     * @return an {@code TransitiveOptional} with a present value if the specified value is
     * non-{@code null}, otherwise an empty {@code TransitiveOptional}
     */
    public static <T> TransitiveOptional<T> ofNullable(T value) {
        return value == null ? empty() : of(value);
    }

    /**
     * Returns an empty {@code TransitiveOptional} instance. No value is present for this {@code
     * TransitiveOptional}.
     *
     * @param <T> The type of the non-existent value
     * @return an empty {@code TransitiveOptional}
     */
    public static <T> TransitiveOptional<T> empty() {
        @SuppressWarnings("unchecked")
        TransitiveOptional<T> t = (TransitiveOptional<T>) EMPTY;
        return t;
    }

    /**
     * If a value is present, returns the value, otherwise throws {@code NoSuchElementException}.
     *
     * @return the non-{@code null} value described by this {@code TransitiveOptional}
     * @throws NoSuchElementException if no value is present
     */
    public T get() {
        if (value == null) {
            throw new NoSuchElementException("No value present");
        }
        return value;
    }

    /**
     * If a value is present, returns an {@code TransitiveOptional} describing the value, otherwise
     * returns an {@code TransitiveOptional} produced by the supplying function.
     *
     * @param supplier the supplying function that produces an {@code TransitiveOptional} to be
     *                 returned
     * @return returns an {@code TransitiveOptional} describing the value of this {@code
     * TransitiveOptional}, if a value is present, otherwise an {@code TransitiveOptional} produced
     * by the supplying function.
     * @throws NullPointerException if the supplying function is {@code null} or produces a {@code
     *                              null} result
     */
    public TransitiveOptional<T> or(Supplier<? extends TransitiveOptional<? extends T>> supplier) {
        Objects.requireNonNull(supplier);
        if (value != null) {
            return this;
        } else {
            @SuppressWarnings("unchecked")
            TransitiveOptional<T> r = (TransitiveOptional<T>) supplier.get();
            return Objects.requireNonNull(r);
        }
    }
}
