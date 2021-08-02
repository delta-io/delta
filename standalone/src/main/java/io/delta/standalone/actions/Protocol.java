package io.delta.standalone.actions;

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
}

