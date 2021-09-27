package io.delta.standalone.actions;

import java.util.Objects;

public class Protocol implements Action {
    private final int minReaderVersion;
    private final int minWriterVersion;

    public Protocol(int minReaderVersion, int minWriterVersion) {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
    }

    public int getMinReaderVersion() {
        return minReaderVersion;
    }

    public int getMinWriterVersion() {
        return minWriterVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Protocol protocol = (Protocol) o;
        return minReaderVersion == protocol.minReaderVersion &&
            minWriterVersion == protocol.minWriterVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minReaderVersion, minWriterVersion);
    }
}

