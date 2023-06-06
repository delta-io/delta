package io.delta.flink.source.internal.file;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.actions.AddFile;

/**
 * The implementation of {@link AddFileEnumerator} for {@link DeltaSourceSplit}.
 * <p>
 * This implementation is converting all discovered Delta's {@link AddFile} objects to set of {@link
 * DeltaSourceSplit}. During the conversion, all {@code AddFiles} are filtered using {@link
 * SplitFilter}
 */
public class DeltaFileEnumerator implements AddFileEnumerator<DeltaSourceSplit> {

    /**
     * The directory separator, a slash.
     */
    public static final String SEPARATOR = "/";

    private static final Logger LOG = LoggerFactory.getLogger(DeltaFileEnumerator.class);

    /**
     * The current Id as a mutable string representation. This covers more values than the integer
     * value range, so we should never overflow.
     */
    // This is copied from Flink's NonSplittingRecursiveEnumerator
    private final char[] currentId = "0000000000".toCharArray();

    /**
     * @param context     {@link AddFileEnumeratorContext} input object for Split conversion.
     * @param splitFilter {@link SplitFilter} instance that will be used to filter out {@link
     *                    AddFile} from split conversion. The {@code SplitFilter} is based on {@link
     *                    Path} representing created from {@link AddFile#getPath()}
     * @return List of {@link DeltaSourceSplit} objects.
     */
    @Override
    public List<DeltaSourceSplit> enumerateSplits(
        AddFileEnumeratorContext context, SplitFilter<Path> splitFilter) {

        ArrayList<DeltaSourceSplit> splitsToReturn = new ArrayList<>(context.getAddFiles().size());

        for (AddFile addFile : context.getAddFiles()) {
            Path path = acquireFilePath(context.getTablePath(), addFile);
            if (splitFilter.test(path)) {
                tryConvertToSourceSplits(context, splitsToReturn, addFile, path);
            }
        }

        return splitsToReturn;
    }

    private void tryConvertToSourceSplits(
        AddFileEnumeratorContext context, ArrayList<DeltaSourceSplit> splitsToReturn,
        AddFile addFile, Path path) {
        try {
            FileSystem fs = path.getFileSystem();
            FileStatus status = fs.getFileStatus(path);
            convertToSourceSplits(status, fs, addFile.getPartitionValues(), splitsToReturn);
        } catch (IOException e) {
            throw DeltaSourceExceptions.fileEnumerationException(context, path, e);
        }
    }

    @VisibleForTesting
    Path acquireFilePath(String tablePath, AddFile addFile) {
        String addFilePath = addFile.getPath();
        URI addFileUri = URI.create(addFilePath);
        if (!addFileUri.isAbsolute()) {
            addFileUri = URI.create(getTablePath(tablePath) + addFilePath);
        }
        return new Path(addFileUri);
    }

    private String getTablePath(String tablePath) {
        // When we deserialize DeltaTablePath as string during recovery,
        // Flink's Path(String path) constructor removes the last '/' from the String.
        return (tablePath.endsWith(SEPARATOR))
            ? tablePath : tablePath + SEPARATOR;
    }

    // ------------------------------------------------------------------------
    //  Copied from Flink's BlockSplittingRecursiveEnumerator and adjusted.
    // ------------------------------------------------------------------------
    private void convertToSourceSplits(final FileStatus fileStatus, final FileSystem fileSystem,
        Map<String, String> partitionValues, final List<DeltaSourceSplit> target)
        throws IOException {

        final BlockLocation[] blocks = getBlockLocationsForFile(fileStatus, fileSystem);
        if (blocks == null) {
            target.add(
                new DeltaSourceSplit(
                    partitionValues,
                    getNextId(),
                    fileStatus.getPath(),
                    0L,
                    fileStatus.getLen()));
        } else {
            for (BlockLocation block : blocks) {
                target.add(new DeltaSourceSplit(
                    partitionValues,
                    getNextId(),
                    fileStatus.getPath(),
                    block.getOffset(),
                    block.getLength(),
                    block.getHosts()));
            }
        }
    }

    @VisibleForTesting
    String getNextId() {
        // because we just increment numbers, we increment the char representation directly,
        // rather than incrementing an integer and converting it to a string representation
        // every time again (requires quite some expensive conversion logic).
        incrementCharArrayByOne(currentId, currentId.length - 1);
        return new String(currentId);
    }

    // ------------------------------------------------------------------------
    //  Copied as is from Flink's BlockSplittingRecursiveEnumerator
    // ------------------------------------------------------------------------
    private void incrementCharArrayByOne(char[] array, int pos) {
        char c = array[pos];
        c++;

        if (c > '9') {
            c = '0';
            incrementCharArrayByOne(array, pos - 1);
        }
        array[pos] = c;
    }

    /**
     * This method will try to get all blocks for given file from underlying File System. If total
     * block size will not match total file size, a warning will be logged and file will not be
     * split to blocks. It will be processed as one.
     */
    private BlockLocation[] getBlockLocationsForFile(FileStatus file, FileSystem fs)
        throws IOException {
        final long len = file.getLen();

        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        if (blocks == null || blocks.length == 0) {
            return null;
        }

        // A cheap check whether we have all blocks.
        // We don't check whether the blocks fully cover the file (too expensive)
        // but make some sanity checks to catch early the common cases where incorrect
        // block info is returned by the implementation.
        long totalLen = 0L;
        for (BlockLocation block : blocks) {
            totalLen += block.getLength();
        }
        if (totalLen != len) {
            LOG.warn(
                "Block lengths do not match file length for {}. File length is {}, blocks are {}",
                file.getPath(), len, Arrays.toString(blocks));
            return null;
        }

        return blocks;
    }
    // ------------------------------------------------------------------------
    //  End of code copied from Flink's BlockSplittingRecursiveEnumerator
    // ------------------------------------------------------------------------
}
