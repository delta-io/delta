/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults;

import static io.delta.kernel.internal.util.VectorUtils.buildArrayValue;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;
import static org.assertj.core.api.Assertions.assertThat;

import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Golden-string tests that pin the raw serialized {@code .crc} JSON produced by kernel
 * (CRCInfo.toRow -> JsonUtils.rowToJson, the same path ChecksumWriter uses via
 * DefaultJsonHandler.writeJsonFileAtomically).
 */
class CRCInfoSerializationTest {

  /** The metadata and protocol prefix shared by every expected JSON string below. */
  private static final String EXPECTED_PREFIX =
      "{\"tableSizeBytes\":100,\"numFiles\":1,\"numMetadata\":1,\"numProtocol\":1,"
          + "\"metadata\":{\"id\":\"id\",\"name\":\"name\",\"description\":\"description\","
          + "\"format\":{\"provider\":\"parquet\",\"options\":{}},"
          + "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[]}\","
          + "\"partitionColumns\":[\"c3\"],"
          + "\"createdTime\":123,\"configuration\":{\"delta.appendOnly\":\"true\"}},"
          + "\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}";

  private static Protocol testProtocol() {
    return new Protocol(1, 2, Collections.emptySet(), Collections.emptySet());
  }

  /** A fully-deterministic Metadata: fixed id, fixed createdTime, single fixed table property. */
  private static Metadata testMetadata() {
    return new Metadata(
        "id",
        Optional.of("name"),
        Optional.of("description"),
        new Format("parquet", Collections.emptyMap()),
        DataTypeJsonSerDe.serializeDataType(new StructType()),
        new StructType(),
        buildArrayValue(Arrays.asList("c3"), StringType.STRING),
        Optional.of(123L),
        stringStringMapValue(Collections.singletonMap("delta.appendOnly", "true")));
  }

  private static String crcJson(CRCInfo crcInfo) {
    return JsonUtils.rowToJson(crcInfo.toRow());
  }

  @Test
  void omitsSetTransactionsWhenAbsent() {
    CRCInfo crcInfo =
        new CRCInfo(
            1L,
            testMetadata(),
            testProtocol(),
            100L, /* tableSizeBytes */
            1L, /* numFiles */
            Optional.empty(), /* txnId */
            Optional.empty(), /* domainMetadata */
            Optional.empty() /* fileSizeHistogram */);

    assertThat(crcJson(crcInfo)).isEqualTo(EXPECTED_PREFIX + "}");
  }

  @Test
  void includesEmptySetTransactionsArray() {
    CRCInfo crcInfo =
        new CRCInfo(
            2L,
            testMetadata(),
            testProtocol(),
            100L,
            1L,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(), /* inCommitTimestamp */
            Optional.of(Collections.<SetTransaction>emptyList()), /* setTransactions */
            Optional.empty(), /* numDeletedRecords */
            Optional.empty(), /* numDeletionVectors */
            Optional.empty() /* allFiles */);

    assertThat(crcJson(crcInfo)).isEqualTo(EXPECTED_PREFIX + ",\"setTransactions\":[]}");
  }

  @Test
  void includesPopulatedSetTransactions() {
    List<SetTransaction> txns =
        Arrays.asList(
            new SetTransaction("app1", 5L, Optional.of(100L)),
            new SetTransaction("app2", 9L, Optional.empty()));
    CRCInfo crcInfo =
        new CRCInfo(
            3L,
            testMetadata(),
            testProtocol(),
            100L,
            1L,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(), /* inCommitTimestamp */
            Optional.of(txns), /* setTransactions */
            Optional.empty(), /* numDeletedRecords */
            Optional.empty(), /* numDeletionVectors */
            Optional.empty() /* allFiles */);

    assertThat(crcJson(crcInfo))
        .isEqualTo(
            EXPECTED_PREFIX
                + ",\"setTransactions\":[{\"appId\":\"app1\",\"version\":5,\"lastUpdated\":100},"
                + "{\"appId\":\"app2\",\"version\":9}]}");
  }
}
