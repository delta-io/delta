package io.delta.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Default implementation of {@link LogStore} for Hadoop {@link FileSystem} implementations.
 */
public abstract class HadoopFileSystemLogStore extends LogStore {

    public HadoopFileSystemLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    public CloseableIterator<String> read(Path path, Configuration hadoopConf) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);
        FSDataInputStream stream = fs.open(path);
        Reader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        return new LineCloseableIterator(reader);
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);
        if (!fs.exists(path.getParent())) {
            throw new FileNotFoundException(
                    String.format("No such file or directory: %s", path.getParent())
            );
        }
        FileStatus[] files = fs.listStatus(path.getParent());
        return Arrays.stream(files)
                .filter(f -> f.getPath().getName().compareTo(path.getName()) >= 0)
                .sorted(Comparator.comparing(o -> o.getPath().getName()))
                .iterator();
    }

    @Override
    public Path resolvePathOnPhysicalStorage(Path path, Configuration hadoopConf) throws IOException {
        return path.getFileSystem(hadoopConf).makeQualified(path);
    }
}
