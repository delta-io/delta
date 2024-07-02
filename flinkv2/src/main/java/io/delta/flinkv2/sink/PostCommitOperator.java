package io.delta.flinkv2.sink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostCommitOperator extends ProcessFunction<CommittableMessage<DeltaCommittable>, Void>  {

    private static final Logger LOG = LoggerFactory.getLogger(PostCommitOperator.class);

    private final String postCommitOperatorId;

    public PostCommitOperator() {
        this.postCommitOperatorId = java.util.UUID.randomUUID().toString();

        LOG.info("PostCommitOperator[{}] > constructor", postCommitOperatorId);
    }

    /*
2024-06-25 10:48:40 INFO  PostCommitOperator:34 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableSummary: org.apache.flink.streaming.api.connector.sink2.CommittableSummary@559bfd53, # committables: 10, checkpointId: OptionalLong[1]
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@527d5c23, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='aa00030f-6ebb-4e07-85c4-b088d717ea72', checkpointId=1, kernelActionRow={"add":{"path":"part1=0/part2=3/e88098d1-5d4a-48f9-8de3-43aec1769c4f-000.parquet","partitionValues":{"part1":"0","part2":"3"},"size":685,"modificationTime":1719337720285,"dataChange":true}}}
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@72e22895, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='2ba929c9-5ea5-48b4-80eb-309334fb7d7f', checkpointId=1, kernelActionRow={"add":{"path":"part1=0/part2=0/c08ca15c-e796-494b-9883-61f91f7655c2-000.parquet","partitionValues":{"part1":"0","part2":"0"},"size":683,"modificationTime":1719337720285,"dataChange":true}}}
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@68838498, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='9232e3b8-804c-4c3f-a2c2-3a6e9a7144b9', checkpointId=1, kernelActionRow={"add":{"path":"part1=0/part2=1/8cf41aa6-2dfd-4fbd-af3c-034b32745e84-000.parquet","partitionValues":{"part1":"0","part2":"1"},"size":685,"modificationTime":1719337720285,"dataChange":true}}}
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@62788dbe, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='d06c1472-e085-4564-a0bd-566a01e86efd', checkpointId=1, kernelActionRow={"add":{"path":"part1=0/part2=4/d055dea5-87f0-4bbb-88be-67da0c1b341a-000.parquet","partitionValues":{"part1":"0","part2":"4"},"size":685,"modificationTime":1719337720285,"dataChange":true}}}
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@5a5c5996, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='6616030e-85ac-4a17-bbc6-60bb47889718', checkpointId=1, kernelActionRow={"add":{"path":"part1=1/part2=0/e4719fba-19e0-494b-8d94-4e38f9ee5e8b-000.parquet","partitionValues":{"part1":"1","part2":"0"},"size":685,"modificationTime":1719337720285,"dataChange":true}}}
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@391964c8, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='b1a3d005-a39e-4bee-951c-0ab83b029e9a', checkpointId=1, kernelActionRow={"add":{"path":"part1=1/part2=2/9a71c759-bd81-400f-97fd-19490a51d576-000.parquet","partitionValues":{"part1":"1","part2":"2"},"size":685,"modificationTime":1719337720285,"dataChange":true}}}
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@4d4a8037, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='b1a3d005-a39e-4bee-951c-0ab83b029e9a', checkpointId=1, kernelActionRow={"add":{"path":"part1=1/part2=1/ddfda72b-5d95-48d2-97b6-9bb704334943-000.parquet","partitionValues":{"part1":"1","part2":"1"},"size":683,"modificationTime":1719337720329,"dataChange":true}}}
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@6bf9f15d, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='eec71547-1a9d-42f7-b8e3-24ebe388a683', checkpointId=1, kernelActionRow={"add":{"path":"part1=1/part2=3/b2eb0f3c-0939-4402-96ef-3788d996a4b1-000.parquet","partitionValues":{"part1":"1","part2":"3"},"size":677,"modificationTime":1719337720285,"dataChange":true}}}
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@5351855d, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='eecc56fb-9796-438e-b1c1-9ccc326a9bd1', checkpointId=1, kernelActionRow={"add":{"path":"part1=1/part2=4/c640381b-df44-46cd-9a1e-ceba96c748d2-000.parquet","partitionValues":{"part1":"1","part2":"4"},"size":685,"modificationTime":1719337720285,"dataChange":true}}}
2024-06-25 10:48:40 INFO  PostCommitOperator:31 - PostCommitOperator[505423a4-04ff-40e1-8169-9df933d6e40e] > processElement > committableWithLineage: org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage@219c94db, deltaCommittable: DeltaCommittable{appId='c51c376d-7b13-4921-827a-8186c6c4cebc', writerId='eecc56fb-9796-438e-b1c1-9ccc326a9bd1', checkpointId=1, kernelActionRow={"add":{"path":"part1=0/part2=2/54f1ed57-5787-488a-8af5-3ec4a1db5b92-000.parquet","partitionValues":{"part1":"0","part2":"2"},"size":683,"modificationTime":1719337720327,"dataChange":true}}}
     */
    @Override
    public void processElement(
            CommittableMessage<DeltaCommittable> value,
            ProcessFunction<CommittableMessage<DeltaCommittable>, Void>.Context ctx,
            Collector<Void> out) throws Exception {
        if (value instanceof CommittableWithLineage) {
            CommittableWithLineage<DeltaCommittable> committableWithLineage = (CommittableWithLineage<DeltaCommittable>) value;
            LOG.info("PostCommitOperator[{}] > processElement > committableWithLineage: {}, deltaCommittable: {}", postCommitOperatorId, committableWithLineage, committableWithLineage.getCommittable());
        } if (value instanceof CommittableSummary){
            CommittableSummary<DeltaCommittable> committableSummary = (CommittableSummary<DeltaCommittable>) value;
            LOG.info("PostCommitOperator[{}] > processElement > committableSummary: {}, # committables: {}, checkpointId: {}", postCommitOperatorId, committableSummary, committableSummary.getNumberOfCommittables(), committableSummary.getCheckpointId());
        }
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        LOG.info("PostCommitOperator[{}] > open", postCommitOperatorId);
    }

    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("PostCommitOperator[{}] > close", postCommitOperatorId);
    }
}
