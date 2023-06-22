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

import java.net.URI;
import java.util.HashMap;
import java.util.Optional;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.actions.Action;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.lang.FilteredCloseableIterator;

public class ReverseActionsToAddFilesIterator
    extends FilteredCloseableIterator<AddFile, Tuple2<Action, Boolean>>
{
    private final Path dataPath;
    private final HashMap<UniqueFileActionTuple, RemoveFile> tombstonesFromJson;
    private final HashMap<UniqueFileActionTuple, AddFile> addFilesFromJson;

    public ReverseActionsToAddFilesIterator(
        Path dataPath,
        CloseableIterator<Tuple2<Action, Boolean>> reverseActionIter)
    {
        super(reverseActionIter);
        this.dataPath = dataPath;
        this.tombstonesFromJson = new HashMap<>();
        this.addFilesFromJson = new HashMap<>();
    }

    @Override
    protected Optional<AddFile> accept(Tuple2<Action, Boolean> element)
    {
        final Action action = element._1;
        final boolean isFromCheckpoint = element._2;

        if (action instanceof AddFile) {
            final AddFile add = ((AddFile) action)
                .copyWithDataChange(false)
                .withAbsolutePath(dataPath);
            final UniqueFileActionTuple key =
                new UniqueFileActionTuple(add.toURI(), add.getDeletionVectorUniqueId());
            final boolean alreadyDeleted = tombstonesFromJson.containsKey(key);
            final boolean alreadyReturned = addFilesFromJson.containsKey(key);

            if (!alreadyReturned) {
                // Note: No AddFile will appear twice in a checkpoint, so we only need
                //       non-checkpoint AddFiles in the set
                if (!isFromCheckpoint) {
                    addFilesFromJson.put(key, add);
                }

                if (!alreadyDeleted) {
                    return Optional.of(add);
                }
            }
        }
        else if (action instanceof RemoveFile && !isFromCheckpoint) {
            // Note: There's no reason to put a RemoveFile from a checkpoint into tombstones map
            //       since, when we generate a checkpoint, any corresponding AddFile would have
            //       been excluded
            final RemoveFile remove = ((RemoveFile) action)
                .copyWithDataChange(false)
                .withAbsolutePath(dataPath);
            final UniqueFileActionTuple key =
                new UniqueFileActionTuple(remove.toURI(), remove.getDeletionVectorUniqueId());

            tombstonesFromJson.put(key, remove);
        }

        return Optional.empty();
    }

    private static class UniqueFileActionTuple
        extends Tuple2<URI, Optional<String>>
    {
        UniqueFileActionTuple(URI fileURI, Optional<String> deletionVectorURI)
        {
            super(fileURI, deletionVectorURI);
        }
    }
}
