package io.delta.standalone.actions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A specification of the encoding for the files stored in a table.
 */
public final class Format {
    private final String provider;
    private final Map<String, String> options;

    public Format(String provider, Map<String, String> options) {
        this.provider = provider;
        this.options = options;
    }

    /**
     * @return the name of the encoding for files in this table
     */
    public String getProvider() {
        return provider;
    }

    /**
     * @return an unmodifiable {@code Map} containing configuration options for
     *         the format
     */
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Format format = (Format) o;
        return Objects.equals(provider, format.provider) &&
                Objects.equals(options, format.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(provider, options);
    }
}
