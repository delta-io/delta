package io.delta.flinkv2.sink;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;

public class DeltaCommittable {
    private final String appId;
    private final String writerId;
    private final long checkpointId;
    private final Row kernelActionRow;

    public DeltaCommittable(String appId, String writeId, long checkpointId, Row kernelActionRow) {
        this.appId = appId;
        this.writerId = writeId;
        this.checkpointId = checkpointId;
        this.kernelActionRow = kernelActionRow;
    }

    public String getAppId() {
        return appId;
    }

    public String getWriterId() {
        return writerId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public Row getKernelActionRow() {
        return kernelActionRow;
    }

    @Override
    public String toString() {
        return "DeltaCommittable{" +
            "appId='" + appId + '\'' +
            ", writerId='" + writerId + '\'' +
            ", checkpointId=" + checkpointId +
            ", kernelActionRow=" + JsonUtils.rowToJson(kernelActionRow) +
            '}';
    }
}
