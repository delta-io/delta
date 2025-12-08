package io.delta.flink.sink;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This DataStream Source waits for all the following conditions to send the finish signal
 * <ul>
 *     <li>The records are exhausted</li>
 *     <li>At least one checkpoint is finished after the records are exhausted</li>
 * </ul>
 * This gives the test environment enough time to finish the last checkpoint with data before
 * the environment shuts down.
 */
public class DelayFinishTestSource<T> implements SourceFunction<T>, CheckpointListener {

    private ArrayList<T> data = new ArrayList<>();
    private volatile boolean exhausted = false;

    private AtomicInteger counter = new AtomicInteger(0);

    public DelayFinishTestSource(List<T> data, int delayCount) {
        this.data.addAll(data);
        this.counter.set(delayCount);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
       if (exhausted) {
           counter.decrementAndGet();
       }
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        data.forEach( item -> {
        synchronized (ctx.getCheckpointLock()) {
                ctx.collect(item);
        }});
        exhausted = true;
        // When the data exhausts, wait for checkpoints
        while (counter.get() > 0) {
            Thread.sleep(100L);
        }
    }

    @Override
    public void cancel() {
        // TODO not implemented
    }
}
