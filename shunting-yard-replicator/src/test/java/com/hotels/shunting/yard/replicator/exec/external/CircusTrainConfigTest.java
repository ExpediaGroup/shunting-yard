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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.hotels.bdp.circustrain.core.conf.ReplicationMode;
import com.hotels.bdp.circustrain.core.conf.TableReplication;

public class CircusTrainConfigTest {

  public @Rule ExpectedException expectedException = ExpectedException.none();

  @Test
  public void withSingleValuePartitionFilter() {
    CircusTrainConfig config = CircusTrainConfig
        .builder()
        .sourceName("sourceName")
        .sourceMetaStoreUri("sourceMetaStoreUri")
        .replicaName("replicaName")
        .replicaMetaStoreUri("replicaMetaStoreUri")
        .copierOption("p1", "val1")
        .copierOption("p2", "val2")
        .replication(ReplicationMode.FULL, "databaseName", "tableName", "replicaTableLocation",
            Arrays.asList(new FieldSchema("part", "string", null)), new List[] { Arrays.asList("partval") })
        .build();
    assertThat(config.getSourceCatalog().getName()).isEqualTo("sourceName");
    assertThat(config.getSourceCatalog().getHiveMetastoreUris()).isEqualTo("sourceMetaStoreUri");
    assertThat(config.getReplicaCatalog().getName()).isEqualTo("replicaName");
    assertThat(config.getReplicaCatalog().getHiveMetastoreUris()).isEqualTo("replicaMetaStoreUri");
    assertThat(config.getCopierOptions()).hasSize(2).containsEntry("p1", "val1").containsEntry("p2", "val2");
    assertThat(config.getTableReplications()).hasSize(1);

    TableReplication replication = config.getTableReplications().get(0);
    assertThat(replication.getReplicationMode()).isEqualTo(ReplicationMode.FULL);
    assertThat(replication.getSourceTable().getDatabaseName()).isEqualTo("databaseName");
    assertThat(replication.getSourceTable().getTableName()).isEqualTo("tableName");
    assertThat(replication.getSourceTable().getPartitionFilter()).isEqualTo("(part='partval')");
    assertThat(replication.getSourceTable().isGeneratePartitionFilter()).isEqualTo(false);
    assertThat(replication.getSourceTable().getPartitionLimit()).isEqualTo(Short.MAX_VALUE);
    assertThat(replication.getReplicaTable().getDatabaseName()).isEqualTo("databaseName");
    assertThat(replication.getReplicaTable().getTableName()).isEqualTo("tableName");
    assertThat(replication.getReplicaTable().getTableLocation()).isEqualTo("replicaTableLocation");
  }

  @Test
  public void withMultiValuePartitionFilter() {
    CircusTrainConfig config = CircusTrainConfig
        .builder()
        .sourceName("sourceName")
        .sourceMetaStoreUri("sourceMetaStoreUri")
        .replicaName("replicaName")
        .replicaMetaStoreUri("replicaMetaStoreUri")
        .copierOption("p1", "val1")
        .copierOption("p2", "val2")
        .replication(ReplicationMode.FULL, "databaseName", "tableName", "replicaTableLocation",
            Arrays.asList(new FieldSchema("part_a", "string", null), new FieldSchema("part_b", "integer", null)),
            new List[] { Arrays.asList("a", "1"), Arrays.asList("a", "2") })
        .build();
    assertThat(config.getSourceCatalog().getName()).isEqualTo("sourceName");
    assertThat(config.getSourceCatalog().getHiveMetastoreUris()).isEqualTo("sourceMetaStoreUri");
    assertThat(config.getReplicaCatalog().getName()).isEqualTo("replicaName");
    assertThat(config.getReplicaCatalog().getHiveMetastoreUris()).isEqualTo("replicaMetaStoreUri");
    assertThat(config.getCopierOptions()).hasSize(2).containsEntry("p1", "val1").containsEntry("p2", "val2");
    assertThat(config.getTableReplications()).hasSize(1);

    TableReplication replication = config.getTableReplications().get(0);
    assertThat(replication.getReplicationMode()).isEqualTo(ReplicationMode.FULL);
    assertThat(replication.getSourceTable().getDatabaseName()).isEqualTo("databaseName");
    assertThat(replication.getSourceTable().getTableName()).isEqualTo("tableName");
    assertThat(replication.getSourceTable().getPartitionFilter())
        .isEqualTo("(part_a='a' AND part_b=1) OR (part_a='a' AND part_b=2)");
    assertThat(replication.getSourceTable().isGeneratePartitionFilter()).isEqualTo(false);
    assertThat(replication.getSourceTable().getPartitionLimit()).isEqualTo(Short.MAX_VALUE);
    assertThat(replication.getReplicaTable().getDatabaseName()).isEqualTo("databaseName");
    assertThat(replication.getReplicaTable().getTableName()).isEqualTo("tableName");
    assertThat(replication.getReplicaTable().getTableLocation()).isEqualTo("replicaTableLocation");
  }

  @Test
  public void withGeneratedPartitionFilter() {
    CircusTrainConfig config = CircusTrainConfig
        .builder()
        .sourceName("sourceName")
        .sourceMetaStoreUri("sourceMetaStoreUri")
        .replicaName("replicaName")
        .replicaMetaStoreUri("replicaMetaStoreUri")
        .replication(ReplicationMode.METADATA_UPDATE, "databaseName", "tableName", "replicaTableLocation")
        .build();
    assertThat(config.getSourceCatalog().getName()).isEqualTo("sourceName");
    assertThat(config.getSourceCatalog().getHiveMetastoreUris()).isEqualTo("sourceMetaStoreUri");
    assertThat(config.getReplicaCatalog().getName()).isEqualTo("replicaName");
    assertThat(config.getReplicaCatalog().getHiveMetastoreUris()).isEqualTo("replicaMetaStoreUri");
    assertThat(config.getCopierOptions()).isEmpty();
    assertThat(config.getTableReplications()).hasSize(1);

    TableReplication replication = config.getTableReplications().get(0);
    assertThat(replication.getReplicationMode()).isEqualTo(ReplicationMode.METADATA_UPDATE);
    assertThat(replication.getSourceTable().getDatabaseName()).isEqualTo("databaseName");
    assertThat(replication.getSourceTable().getTableName()).isEqualTo("tableName");
    assertThat(replication.getSourceTable().getPartitionFilter()).isBlank();
    assertThat(replication.getSourceTable().isGeneratePartitionFilter()).isEqualTo(true);
    assertThat(replication.getSourceTable().getPartitionLimit()).isEqualTo(Short.MAX_VALUE);
    assertThat(replication.getReplicaTable().getDatabaseName()).isEqualTo("databaseName");
    assertThat(replication.getReplicaTable().getTableName()).isEqualTo("tableName");
    assertThat(replication.getReplicaTable().getTableLocation()).isEqualTo("replicaTableLocation");
  }

  @Test
  public void missingSourceCatalogName() {
    CircusTrainConfig config = CircusTrainConfig
        .builder()
        .sourceMetaStoreUri("sourceMetaStoreUri")
        .replicaName("replicaName")
        .replicaMetaStoreUri("replicaMetaStoreUri")
        .replication(ReplicationMode.METADATA_UPDATE, "databaseName", "tableName", "replicaTableLocation")
        .build();
    assertThat(config.getSourceCatalog().getName()).isEqualTo("source");
  }

  @Test
  public void missingReplicaCatalogName() {
    CircusTrainConfig config = CircusTrainConfig
        .builder()
        .sourceName("sourceName")
        .sourceMetaStoreUri("sourceMetaStoreUri")
        .replicaMetaStoreUri("replicaMetaStoreUri")
        .replication(ReplicationMode.METADATA_UPDATE, "databaseName", "tableName", "replicaTableLocation")
        .build();
    assertThat(config.getReplicaCatalog().getName()).isEqualTo("replica");
  }

  @Test
  public void missingSourceMetasStoreUri() {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("sourceMetaStoreUri is not set");
    CircusTrainConfig
        .builder()
        .sourceName("sourceName")
        .replicaName("replicaName")
        .replicaMetaStoreUri("replicaMetaStoreUri")
        .replication(ReplicationMode.METADATA_UPDATE, "databaseName", "tableName", "replicaTableLocation")
        .build();
  }

  @Test
  public void missingReplicaMetasStoreUri() {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("replicaMetaStoreUri is not set");
    CircusTrainConfig
        .builder()
        .sourceName("sourceName")
        .sourceMetaStoreUri("sourceMetaStoreUri")
        .replicaName("replicaName")
        .replication(ReplicationMode.METADATA_UPDATE, "databaseName", "tableName", "replicaTableLocation")
        .build();
  }

  @Test
  public void missingReplications() {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("tableReplications is not set");
    CircusTrainConfig
        .builder()
        .sourceName("sourceName")
        .sourceMetaStoreUri("sourceMetaStoreUri")
        .replicaName("replicaName")
        .replicaMetaStoreUri("replicaMetaStoreUri")
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void nullSourceCatalogName() {
    CircusTrainConfig.builder().sourceName(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullSourceMetaStoreUri() {
    CircusTrainConfig.builder().sourceMetaStoreUri(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullReplicaCatalogName() {
    CircusTrainConfig.builder().replicaName(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullReplicaMetaStoreUri() {
    CircusTrainConfig.builder().replicaMetaStoreUri(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullReplicationMode() {
    CircusTrainConfig.builder().replication(null, "databaseName", "tableName", "replicaTableLocation");
  }

  @Test(expected = NullPointerException.class)
  public void nullDatabaseName() {
    CircusTrainConfig.builder().replication(ReplicationMode.METADATA_MIRROR, null, "tableName", "replicaTableLocation");
  }

  @Test(expected = NullPointerException.class)
  public void nullTableName() {
    CircusTrainConfig.builder().replication(ReplicationMode.METADATA_MIRROR, "databaseName", null,
        "replicaTableLocation");
  }

  @Test(expected = NullPointerException.class)
  public void nullReplicaTableLocation() {
    CircusTrainConfig.builder().replication(ReplicationMode.METADATA_MIRROR, "databaseName", "tableName", null);
  }

}
