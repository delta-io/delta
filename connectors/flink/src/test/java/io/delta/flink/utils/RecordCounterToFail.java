package io.delta.flink.utils;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class for {@link DataStream} that counts number of processed records and for each
 * execute {@link FailCheck}. The client of {@code RecordCounterToFail} can wait on {@code
 * CompletableFuture} by calling {@link RecordCounterToFail#waitToFail()} method. Method returns
 * whenever {@link FailCheck} passes and completes dedicated "fail" {@link CompletableFuture}.
 */
public class RecordCounterToFail implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(RecordCounterToFail.class);

    private static AtomicInteger records;

    private static CompletableFuture<Void> fail;

    private static CompletableFuture<Void> continueProcessing;

    /**
     * This method counts number of processed records from provided {@link DataStream } and for each
     * record execute {@link FailCheck}. Fail check will trigger at most only one fail.complete()
     * invocation.
     */
    public static <T> DataStream<T> wrapWithFailureAfter(DataStream<T> stream,
        FailCheck failCheck) {

        records = new AtomicInteger();
        fail = new CompletableFuture<>();
        continueProcessing = new CompletableFuture<>();

        return stream.map(
            record -> {
                boolean notFailedYet = !fail.isDone();
                int processedCount = records.incrementAndGet();
                if (notFailedYet && failCheck.test(processedCount)) {
                    fail.complete(null);
                    continueProcessing.get();
                }
                return record;
            });
    }

    /**
     * Wait until dedicated "fail" {@link CompletableFuture} will be released.
     */
    public static void waitToFail() throws Exception {
        fail.get();
        LOG.info("Fail.get finished.");
    }

    /**
     * Allows to furher processing after passing {@link FailCheck} used for {@link
     * RecordCounterToFail#wrapWithFailureAfter(DataStream, FailCheck)}.
     */
    public static void continueProcessing() {
        continueProcessing.complete(null);
    }

    /**
     * A {@link Predicate} used to mark when "stream fail" can take place.
     *
     * @implNote We need to extend Serializable interface to allow Flink serialize Lambda
     * expression. Alternative would be adding (Predicate<Integer> & Serializable) cast to method
     * call, which does not look good.
     */
    @FunctionalInterface
    public interface FailCheck extends Predicate<Integer>, Serializable {

    }
}
