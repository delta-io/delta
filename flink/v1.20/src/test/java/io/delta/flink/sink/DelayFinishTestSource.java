/*
 *  Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * This DataStream Source waits for all the following conditions to send the finish signal
 *
 * <ul>
 *   <li>The records are exhausted
 *   <li>At least one checkpoint is finished after the records are exhausted
 * </ul>
 *
 * This gives the test environment enough time to finish the last checkpoint with data before the
 * environment shuts down.
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
    data.forEach(
        item -> {
          synchronized (ctx.getCheckpointLock()) {
            ctx.collect(item);
          }
        });
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
