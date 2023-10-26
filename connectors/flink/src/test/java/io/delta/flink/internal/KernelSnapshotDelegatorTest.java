package io.delta.flink.internal;

import java.io.IOException;
import java.util.Optional;

import io.delta.flink.utils.DeltaTestUtils;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.Protocol;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.internal.SnapshotImpl;
import io.delta.standalone.internal.KernelDeltaLogDelegator;
import io.delta.standalone.internal.scan.DeltaScanImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.rules.TemporaryFolder;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class KernelSnapshotDelegatorTest {
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

    private KernelDeltaLogDelegator getLog() throws Exception {
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);
        return KernelDeltaLogDelegator.forTable(
            new Configuration(),
            sourceTablePath
            );
    }
    
    @Test
    public void testSnapshotVersion() throws Exception {
        KernelDeltaLogDelegator kernelDeltaLog = getLog();
        SnapshotImpl snapshot = kernelDeltaLog.snapshot();
        assertEquals(snapshot.getVersion(), 0l);
    }

    @Test
    public void testMetadata() throws Exception {
        KernelDeltaLogDelegator kernelDeltaLog = getLog();
        SnapshotImpl snapshot = kernelDeltaLog.snapshot();
        Metadata metadata = snapshot.getMetadata();
        assertEquals(metadata.getId(), "eaf79abd-f74f-4618-a013-afd3a69c7ad2");
        assertEquals(metadata.getName(), null);
        Optional<Long> created = metadata.getCreatedTime();
        assertThat(created.isPresent());
        assertEquals(created.get(), 1641940596848L);
    }

    @Test
    public void testProtocol() throws Exception {
        KernelDeltaLogDelegator kernelDeltaLog = getLog();
        SnapshotImpl snapshot = kernelDeltaLog.snapshot();
        Protocol protocol = snapshot.protocol();
        assertEquals(protocol.getMinReaderVersion(), 1);
        assertEquals(protocol.getMinWriterVersion(), 2);
    }

    @Test
    public void testNumOfFiles() throws Exception {
        KernelDeltaLogDelegator kernelDeltaLog = getLog();
        SnapshotImpl snapshot = kernelDeltaLog.snapshot();
        Long n = snapshot.numOfFiles();
        assertEquals(n, 1);
    }

    @Test
    public void testScanScala() throws Exception {
        KernelDeltaLogDelegator kernelDeltaLog = getLog();
        SnapshotImpl snapshot = kernelDeltaLog.snapshot();
        DeltaScanImpl scan = snapshot.scanScala();
        CloseableIterator<AddFile> files = scan.getFiles();
        assertThat(files.hasNext());
        files.next();
        assertThat(!files.hasNext());
    }

    // Just check these don't throw as the types get hairy
    @Test
    public void testCanCallScalaMethods() throws Exception {
        KernelDeltaLogDelegator kernelDeltaLog = getLog();
        SnapshotImpl snapshot = kernelDeltaLog.snapshot();
        snapshot.setTransactionsScala();
        snapshot.allFilesScala();
    }
}
