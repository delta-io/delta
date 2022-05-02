package io.delta.flink.source.internal.enumerator;

import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith(MockitoJUnitRunner.class)
public class BoundedSplitEnumeratorProviderTest {

    @Mock
    private FileSplitAssigner.Provider fileSplitAssignerProvider;

    @Mock
    private AddFileEnumerator.Provider<DeltaSourceSplit> addFileEnumeratorProvider;

    private BoundedSplitEnumeratorProvider provider;

    @Before
    public void setUp() {
        provider = new BoundedSplitEnumeratorProvider(fileSplitAssignerProvider,
            addFileEnumeratorProvider);
    }

    @Test
    public void shouldReturnBoundedMode() {
        assertThat(provider.getBoundedness(), equalTo(Boundedness.BOUNDED));
    }
}
