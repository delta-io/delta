package io.delta.flink.source.internal.enumerator;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;
import org.mockito.stubbing.Answer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class SourceSplitEnumeratorTestUtils {

    // Mock FileEnumerator to check and execute SplitFilter instance used by
    // BoundedDeltaSourceSplitEnumerator.
    public static void mockFileEnumerator(AddFileEnumerator<DeltaSourceSplit> fileEnumerator) {
        when(fileEnumerator.enumerateSplits(any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)))
            .thenAnswer((Answer<List<DeltaSourceSplit>>) invocation -> {
                AddFileEnumeratorContext context = invocation.getArgument(0);
                SplitFilter<Path> filter = invocation.getArgument(1);

                // id is not a primitive int just to trick Java
                // since we need to use final objects in streams.
                AtomicInteger id = new AtomicInteger(0);
                return context.getAddFiles().stream()
                    .filter(addFile -> filter.test(new Path(addFile.getPath())))
                    .map(addFile ->
                        new DeltaSourceSplit(addFile.getPartitionValues(),
                            String.valueOf(id.incrementAndGet()), new Path(addFile.getPath()),
                            0L, 0L))
                    .collect(Collectors.toList());
            });
    }

    public static List<DeltaSourceSplit> mockSplits() {
        return Arrays.asList(mock(DeltaSourceSplit.class), mock(DeltaSourceSplit.class),
            mock(DeltaSourceSplit.class),
            mock(DeltaSourceSplit.class));
    }
}
