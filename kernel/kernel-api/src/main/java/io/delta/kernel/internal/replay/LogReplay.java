/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.replay;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.actions.Action;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.CloseableIterable;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.snapshot.LogSegment;

public class LogReplay
{
    private final Path dataPath;
    private final LogSegment logSegment;
    private final CloseableIterable<Tuple2<Action, Boolean>> reverseActionsIterable;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;

    public LogReplay(
        Path logPath,
        Path dataPath,
        TableClient tableHelper,
        LogSegment logSegment)
    {
        this.dataPath = dataPath;
        this.logSegment = logSegment;

        final Stream<FileStatus> allFiles = Stream.concat(
            logSegment.checkpoints.stream(),
            logSegment.deltas.stream());
        assertLogFilesBelongToTable(logPath, allFiles);

        this.reverseActionsIterable = new ReverseFilesToActionsIterable(
            tableHelper,
            allFiles);
        this.protocolAndMetadata = new Lazy<>(this::loadTableProtocolAndMetadata);
    }

    public Lazy<Tuple2<Protocol, Metadata>> lazyLoadProtocolAndMetadata()
    {
        return this.protocolAndMetadata;
    }

    public CloseableIterator<AddFile> getAddFiles()
    {
        final CloseableIterator<Tuple2<Action, Boolean>> reverseActionsIter =
            reverseActionsIterable.iterator();
        return new ReverseActionsToAddFilesIterator(dataPath, reverseActionsIter);
    }

    private Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata()
    {
        Protocol protocol = null;
        Metadata metadata = null;

        try (CloseableIterator<Tuple2<Action, Boolean>> reverseIter =
            reverseActionsIterable.iterator()) {
            while (reverseIter.hasNext()) {
                final Action action = reverseIter.next()._1;

                if (action instanceof Protocol && protocol == null) {
                    // We only need the latest protocol
                    protocol = (Protocol) action;

                    if (metadata != null) {
                        // Stop since we have found the latest Protocol and Metadata.
                        validateSupportedTable(protocol, metadata);
                        return new Tuple2<>(protocol, metadata);
                    }
                }
                else if (action instanceof Metadata && metadata == null) {
                    // We only need the latest Metadata
                    metadata = (Metadata) action;

                    if (protocol != null) {
                        // Stop since we have found the latest Protocol and Metadata.
                        validateSupportedTable(protocol, metadata);
                        return new Tuple2<>(protocol, metadata);
                    }
                }
            }
        }
        catch (IOException ex) {
            throw new RuntimeException("Could not close iterator", ex);
        }

        if (protocol == null) {
            throw new IllegalStateException(
                String.format("No protocol found at version %s", logSegment.version)
            );
        }

        throw new IllegalStateException(
            String.format("No metadata found at version %s", logSegment.version)
        );
    }

    private void validateSupportedTable(Protocol protocol, Metadata metadata)
    {
        switch (protocol.getMinReaderVersion()) {
            case 1:
                break;
            case 2:
                verifySupportedColumnMappingMode(metadata);
                break;
            case 3:
                List<String> readerFeatures = protocol.getReaderFeatures();
                for (String readerFeature : readerFeatures) {
                    switch (readerFeature) {
                        case "deletionVectors":
                            break;
                        case "columnMapping":
                            verifySupportedColumnMappingMode(metadata);
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                "Unsupported table feature: " + readerFeature);
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported protocol version: " + protocol.getMinReaderVersion());
        }
    }

    private void verifySupportedColumnMappingMode(Metadata metadata) {
        // Check if the mode is name. Id mode is not yet supported
        String cmMode = metadata.getConfiguration().get("delta.columnMapping.mode");
        if (!"none".equalsIgnoreCase(cmMode) &&
            !"name".equalsIgnoreCase(cmMode)) {
            throw new UnsupportedOperationException(
                "Unsupported column mapping mode: " + cmMode);
        }
    }

    /**
     * Verifies that a set of delta or checkpoint files to be read actually belongs to this table.
     */
    private void assertLogFilesBelongToTable(Path logPath, Stream<FileStatus> allFiles)
    {
        // TODO:
    }
}
