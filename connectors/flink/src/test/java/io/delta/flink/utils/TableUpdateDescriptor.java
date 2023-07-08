package io.delta.flink.utils;

/**
 * A POJO class containing information how many new versions and how many rows per version should be
 * Delta table updated with during execution IT tests.
 */
public class TableUpdateDescriptor {

    private final int numberOfNewVersions;

    private final int numberOfRecordsPerNewVersion;

    public TableUpdateDescriptor(int numberOfNewVersions, int numberOfRecordsPerNewVersion) {
        this.numberOfNewVersions = numberOfNewVersions;
        this.numberOfRecordsPerNewVersion = numberOfRecordsPerNewVersion;
    }

    public int getNumberOfNewVersions() {
        return numberOfNewVersions;
    }

    public int getNumberOfRecordsPerNewVersion() {
        return numberOfRecordsPerNewVersion;
    }

    @Override
    public String toString() {
        return "TableUpdateDescriptor{" +
            "numberOfNewVersions=" + numberOfNewVersions +
            ", numberOfRecordsPerNewVersion=" + numberOfRecordsPerNewVersion +
            '}';
    }
}
