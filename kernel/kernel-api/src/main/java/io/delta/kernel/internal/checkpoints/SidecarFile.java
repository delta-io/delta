package io.delta.kernel.internal.checkpoints;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;

import java.util.Objects;

public class SidecarFile {
    public String path;
    public long sizeInBytes;
    public long modificationTime;

    public static SidecarFile fromRow(Row row) {
        return new SidecarFile(
                row.getString(0),
                row.getLong(1),
                row.getLong(2)
        );
    }

    public SidecarFile(String path, long sizeInBytes, long modificationTime) {
        this.path = path;
        this.sizeInBytes = sizeInBytes;
        this.modificationTime = modificationTime;
    }

    public SidecarFile(FileStatus fileStatus) {
        this(fileStatus.getPath(), fileStatus.getSize(), fileStatus.getModificationTime());
    }

    public static StructType READ_SCHEMA = new StructType()
            .add("path", StringType.STRING, false /* nullable */)
            .add("sizeInBytes", LongType.LONG, false /* nullable */)
            .add("modificationTime", LongType.LONG, false /* nullable */);

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SidecarFile otherSidecarFile = (SidecarFile) other;
        return this.path.equals(otherSidecarFile.path) &&
                this.sizeInBytes == otherSidecarFile.sizeInBytes &&
                this.modificationTime == otherSidecarFile.modificationTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, sizeInBytes, modificationTime);
    }
}
