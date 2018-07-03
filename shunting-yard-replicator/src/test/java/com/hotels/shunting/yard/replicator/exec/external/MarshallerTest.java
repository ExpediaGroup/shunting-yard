/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.shunting.yard.replicator.exec.external;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.io.Files;

import com.hotels.bdp.circustrain.conf.ReplicationMode;

public class MarshallerTest {

  public @Rule TemporaryFolder tmp = new TemporaryFolder();

  private final Marshaller marshaller = new Marshaller();

  @Test
  public void typical() throws Exception {
    CircusTrainConfig config = CircusTrainConfig
        .builder()
        .sourceName("sourceName")
        .sourceMetaStoreUri("sourceMetaStoreUri")
        .replicaName("replicaName")
        .replicaMetaStoreUri("replicaMetaStoreUri")
        .copierOption("p1", "val1")
        .copierOption("p2", "val2")
        .replication(ReplicationMode.FULL, "databaseName", "tableName", "replicaTableLocation", Arrays.asList("part"),
            Arrays.asList(Arrays.asList("partval")))
        .build();

    File file = tmp.newFile("conif.yml");
    marshaller.marshall(file.getAbsolutePath(), config);
    assertThat(Files.readLines(file, StandardCharsets.UTF_8))
        .contains("source-catalog:")
        .contains("  disable-snapshots: false")
        .contains("  name: sourceName")
        .contains("  hive-metastore-uris: sourceMetaStoreUri")
        .contains("replica-catalog:")
        .contains("  name: replicaName")
        .contains("  hive-metastore-uris: replicaMetaStoreUri")
        .contains("copier-options:")
        .contains("  p1: val1")
        .contains("  p2: val2")
        .contains("table-replications:")
        .contains("  replication-mode: FULL")
        .contains("  source-table:")
        .contains("    database-name: databaseName")
        .contains("    table-name: tableName")
        .contains("    partition-filter: (part='partval')")
        .contains("    partition-limit: 32767")
        .contains("  replica-table:")
        .contains("    database-name: databaseName")
        .contains("    table-name: tableName")
        .contains("    table-location: replicaTableLocation");
  }

  @Test
  public void generatePartitionFilter() throws Exception {
    CircusTrainConfig config = CircusTrainConfig
        .builder()
        .sourceName("sourceName")
        .sourceMetaStoreUri("sourceMetaStoreUri")
        .replicaName("replicaName")
        .replicaMetaStoreUri("replicaMetaStoreUri")
        .copierOption("p1", "val1")
        .copierOption("p2", "val2")
        .replication(ReplicationMode.FULL, "databaseName", "tableName", "replicaTableLocation")
        .build();

    File file = tmp.newFile("conif.yml");
    marshaller.marshall(file.getAbsolutePath(), config);
    assertThat(Files.readLines(file, StandardCharsets.UTF_8))
        .contains("source-catalog:")
        .contains("  disable-snapshots: false")
        .contains("  name: sourceName")
        .contains("  hive-metastore-uris: sourceMetaStoreUri")
        .contains("replica-catalog:")
        .contains("  name: replicaName")
        .contains("  hive-metastore-uris: replicaMetaStoreUri")
        .contains("copier-options:")
        .contains("  p1: val1")
        .contains("  p2: val2")
        .contains("table-replications:")
        .contains("  replication-mode: FULL")
        .contains("  source-table:")
        .contains("    database-name: databaseName")
        .contains("    table-name: tableName")
        .contains("    generate-partition-filter: true")
        .contains("    partition-limit: 32767")
        .contains("  replica-table:")
        .contains("    database-name: databaseName")
        .contains("    table-name: tableName")
        .contains("    table-location: replicaTableLocation");
  }

}
