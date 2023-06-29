package io.delta.flink.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public class DeltaTableAsserts {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaTableAsserts.class);


    public static DeltaLogAsserter assertThat(DeltaLog deltaLog) {
        return new DeltaLogAsserter(deltaLog);
    }

    public static class DeltaLogAsserter {

        private final DeltaLog deltaLog;

        private DeltaLogAsserter(DeltaLog deltaLog) {
            this.deltaLog = deltaLog;
        }

        public DeltaLogAsserter hasNoDataLoss(String uniqueColumnName) {
            List<Integer> data = new LinkedList<>();

            int maxValue = 0;

            try (CloseableIterator<RowRecord> open = deltaLog.snapshot().open()) {
                while (open.hasNext()) {
                    RowRecord record = open.next();
                    String f1 = record.getString(uniqueColumnName);
                    int value = Integer.parseInt(f1);
                    data.add(value);
                    if (value > maxValue) {
                        maxValue = value;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            Collections.sort(data);
            if (data.size() <= 5000) {
                LOG.info("#############");
                for (Integer value : data) {
                    LOG.info(String.format("Column %s value %s", uniqueColumnName, value));
                }
                LOG.info("#############");
            }

            LOG.info("Number of entries " + data.size());
            org.assertj.core.api.Assertions.assertThat(data)
                .withFailMessage("Delta table has no data.")
                .isNotEmpty();

            org.assertj.core.api.Assertions.assertThat(maxValue)
                .withFailMessage("Data loss")
                .isEqualTo(data.size() - 1);
            return this;
        }

        public DeltaLogAsserter hasNoDuplicateAddFiles() {

            Iterator<VersionLog> changes = deltaLog.getChanges(0, true);
            final Map<String, StringJoiner> filesFromLog = new HashMap<>();
            boolean wasDuplicate = false;
            boolean hadData = false;

            while (changes.hasNext()) {
                final VersionLog versionLog = changes.next();
                final String currentVersion = String.valueOf(versionLog.getVersion());

                try (io.delta.storage.CloseableIterator<Action> actionsIterator =
                    versionLog.getActionsIterator()) {
                    while (actionsIterator.hasNext()) {
                        Action action = actionsIterator.next();
                        if (action instanceof AddFile) {
                            hadData = true;
                            String filePath = ((AddFile) action).getPath();
                            StringJoiner ifPresent = filesFromLog.computeIfPresent(filePath,
                                (path, stringJoiner) -> stringJoiner.add(currentVersion));
                            if (ifPresent == null) {
                                filesFromLog.computeIfAbsent(filePath,
                                    path -> new StringJoiner(", ").add(currentVersion));
                            } else {
                                LOG.info("File was added more than once " + filePath);
                                wasDuplicate = true;
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            for (Entry<String, StringJoiner> entry : filesFromLog.entrySet()) {
                LOG.info(
                    String.format("File [%s], was added to version [%s]",
                        entry.getKey(), entry.getValue())
                );
            }

            org.assertj.core.api.Assertions.assertThat(hadData)
                .withFailMessage("Delta table has no data.")
                .isTrue();
            org.assertj.core.api.Assertions.assertThat(wasDuplicate)
                .withFailMessage("Seems there was a duplicated AddFile in Delta log")
                .isFalse();

            return this;
        }
    }

}
