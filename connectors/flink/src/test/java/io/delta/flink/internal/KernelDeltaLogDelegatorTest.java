package io.delta.flink.internal;

import java.io.IOException;
import java.util.Iterator;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.rules.TemporaryFolder;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.standalone.VersionLog;
import io.delta.standalone.internal.KernelDeltaLogDelegator;
import io.delta.standalone.internal.SnapshotImpl;

@ExtendWith(MockitoExtension.class)
class KernelDeltaLogDelegatorTest {
    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
    private KernelDeltaLogDelegator kernelDeltaLog;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @Test
    public void testKernelDetectsTable() throws Exception {
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);
        KernelDeltaLogDelegator kernelDeltaLog = KernelDeltaLogDelegator.forTable(
            new Configuration(),
            sourceTablePath
        );
        assertThat(kernelDeltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder.")
            .isTrue();
    }

    @Test
    public void testSnapshot() throws Exception {
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);
        KernelDeltaLogDelegator kernelDeltaLog = KernelDeltaLogDelegator.forTable(
            new Configuration(),
            sourceTablePath
        );
        SnapshotImpl snapshot = kernelDeltaLog.snapshot();
        assertEquals(snapshot.getVersion(), 0L);
    }

    @Test
    public void testGetChanges() throws Exception {
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);
        KernelDeltaLogDelegator kernelDeltaLog = KernelDeltaLogDelegator.forTable(
            new Configuration(),
            sourceTablePath
        );
        Iterator<VersionLog> changes = kernelDeltaLog.getChanges(0, true);
        assertThat(changes.hasNext());
    }
}
