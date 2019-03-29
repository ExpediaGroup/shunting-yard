/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.shunting.yard.replicator.exec.receiver;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import static com.hotels.shunting.yard.replicator.exec.app.ConfigurationVariables.CT_CONFIG;
import static com.hotels.shunting.yard.replicator.exec.app.ConfigurationVariables.WORKSPACE;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.expedia.apiary.extensions.receiver.common.event.EventType;

import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.replicator.exec.conf.ShuntingYardTableReplications;
import com.hotels.shunting.yard.replicator.exec.conf.ct.SyReplicaTable;
import com.hotels.shunting.yard.replicator.exec.conf.ct.SyTableReplication;
import com.hotels.shunting.yard.replicator.exec.conf.ct.SyTableReplications;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.external.CircusTrainConfig;
import com.hotels.shunting.yard.replicator.exec.external.Marshaller;

@RunWith(MockitoJUnitRunner.class)
public class ContextFactoryTest {

  private static final String CT_EVENT_ID = "cte-20180509t111925.123z-aBc123Zo";
  private static final String SOURCE_METASTORE_URIS = "sourceMetastoreUris";
  private static final String REPLICA_METASTORE_URIS = "replicaMetastoreUris";
  private static final String REPLICA_DATABASE_LOCATION = "replicaDatabaseLocation";
  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final String REPLICA_DATABASE = "replica_db";
  private static final String REPLICA_TABLE = "replica_tbl";

  public @Rule TemporaryFolder tmp = new TemporaryFolder();

  private @Mock CloseableMetaStoreClient replicaMetaStoreClient;
  private @Mock Marshaller marshaller;
  private @Mock MetaStoreEvent event;
  private @Mock Table replicaTable;
  private @Mock StorageDescriptor replicaStorageDescriptor;
  private @Mock Map<String, String> eventParameters;

  private @Captor ArgumentCaptor<CircusTrainConfig> circusTrainConfigCaptor;

  private final Configuration conf = new Configuration();
  private File replicaTableLocation;
  private File workspaceDir;
  private String actualWorkspacePattern;
  private ContextFactory factory;

  @Before
  public void init() throws Exception {
    workspaceDir = tmp.newFolder("workspace");
    actualWorkspacePattern = workspaceDir.getAbsolutePath() + "/CREATE_TABLE_\\d{8}T\\d{9}";
    conf.set(WORKSPACE.key(), workspaceDir.getAbsolutePath());
    conf.set(METASTOREURIS.varname, REPLICA_METASTORE_URIS);

    when(event.getParameters()).thenReturn(eventParameters);
    when(event.getEventType()).thenReturn(EventType.CREATE_TABLE);
    when(event.getDatabaseName()).thenReturn(DATABASE);
    when(event.getTableName()).thenReturn(TABLE);
    when(event.getReplicationMode()).thenReturn(ReplicationMode.FULL);
    when(eventParameters.get(METASTOREURIS.varname)).thenReturn(SOURCE_METASTORE_URIS);

    replicaTableLocation = tmp.newFolder(CT_EVENT_ID);
    when(replicaStorageDescriptor.getLocation()).thenReturn(replicaTableLocation.getAbsolutePath());

    when(replicaTable.getSd()).thenReturn(replicaStorageDescriptor);

    when(replicaMetaStoreClient.getTable(DATABASE, TABLE)).thenReturn(replicaTable);
    when(replicaMetaStoreClient.getTable(REPLICA_DATABASE, REPLICA_TABLE)).thenReturn(replicaTable);

    factory = new ContextFactory(conf, replicaMetaStoreClient, marshaller, new ShuntingYardTableReplications());
  }

  @Test
  public void createContextForUnpartitionedTable() {
    Context context = factory.createContext(event);
    assertThat(context.getWorkspace()).matches(actualWorkspacePattern);
    assertThat(context.getConfigLocation()).matches(actualWorkspacePattern + "/replication.yml");
    verify(marshaller).marshall(eq(context.getConfigLocation()), circusTrainConfigCaptor.capture());
    CircusTrainConfig circusTrainConfig = circusTrainConfigCaptor.getValue();
    assertCircusTrainConfig(circusTrainConfig);
    TableReplication replication = circusTrainConfig.getTableReplications().get(0);
    assertTableReplication(replication);
    assertThat(replication.getReplicationMode()).isSameAs(event.getReplicationMode());
    assertThat(replication.getSourceTable().getPartitionFilter()).isBlank();
    assertThat(replication.getReplicaTable().getTableLocation())
        .isEqualTo(replicaTableLocation.getParentFile().getAbsolutePath());
    assertThat(context.getCircusTrainConfigLocation()).isNull();
  }

  @Test
  public void createContextForPartitionedTable() {
    when(event.getPartitionColumns()).thenReturn(Arrays.asList("s", "i"));
    when(event.getPartitionValues()).thenReturn(Arrays.asList(Arrays.asList("a", "1"), Arrays.asList("b", "2")));
    Context context = factory.createContext(event);
    assertThat(context.getWorkspace()).matches(actualWorkspacePattern);
    assertThat(context.getConfigLocation()).matches(actualWorkspacePattern + "/replication.yml");
    verify(marshaller).marshall(eq(context.getConfigLocation()), circusTrainConfigCaptor.capture());
    CircusTrainConfig circusTrainConfig = circusTrainConfigCaptor.getValue();
    assertCircusTrainConfig(circusTrainConfig);
    TableReplication replication = circusTrainConfig.getTableReplications().get(0);
    assertTableReplication(replication);
    assertThat(replication.getReplicationMode()).isSameAs(event.getReplicationMode());
    assertThat(replication.getSourceTable().getPartitionFilter()).isEqualTo("(s='a' AND i='1') OR (s='b' AND i='2')");
    assertThat(replication.getReplicaTable().getTableLocation())
        .isEqualTo(replicaTableLocation.getParentFile().getAbsolutePath());
    assertThat(context.getCircusTrainConfigLocation()).isNull();
  }

  @Test
  public void createContextForPartitionedTableWithReplicaTableSpecified() {
    SyTableReplication tableReplication = new SyTableReplication();
    SourceTable sourceTable = new SourceTable();
    sourceTable.setDatabaseName(DATABASE);
    sourceTable.setTableName(TABLE);

    SyReplicaTable replicaTable = new SyReplicaTable();
    replicaTable.setDatabaseName(REPLICA_DATABASE);
    replicaTable.setTableName(REPLICA_TABLE);

    tableReplication.setSourceTable(sourceTable);
    tableReplication.setReplicaTable(replicaTable);

    List<SyTableReplication> tableReplications = new ArrayList<>();
    tableReplications.add(tableReplication);

    SyTableReplications tableReplicationsWrapper = new SyTableReplications();
    tableReplicationsWrapper.setTableReplications(tableReplications);

    factory = new ContextFactory(conf, replicaMetaStoreClient, marshaller,
        new ShuntingYardTableReplications(tableReplicationsWrapper));

    when(event.getPartitionColumns()).thenReturn(Arrays.asList("s", "i"));
    when(event.getPartitionValues()).thenReturn(Arrays.asList(Arrays.asList("a", "1"), Arrays.asList("b", "2")));
    Context context = factory.createContext(event);
    assertThat(context.getWorkspace()).matches(actualWorkspacePattern);
    assertThat(context.getConfigLocation()).matches(actualWorkspacePattern + "/replication.yml");
    verify(marshaller).marshall(eq(context.getConfigLocation()), circusTrainConfigCaptor.capture());
    CircusTrainConfig circusTrainConfig = circusTrainConfigCaptor.getValue();
    assertCircusTrainConfig(circusTrainConfig);

    TableReplication replication = circusTrainConfig.getTableReplications().get(0);

    assertThat(replication.getSourceTable().getDatabaseName()).isEqualTo(DATABASE);
    assertThat(replication.getSourceTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getReplicaTable().getDatabaseName()).isEqualTo(REPLICA_DATABASE);
    assertThat(replication.getReplicaTable().getTableName()).isEqualTo(REPLICA_TABLE);
    assertThat(replication.getReplicationMode()).isSameAs(event.getReplicationMode());
    assertThat(replication.getSourceTable().getPartitionFilter()).isEqualTo("(s='a' AND i='1') OR (s='b' AND i='2')");
    assertThat(replication.getReplicaTable().getTableLocation())
        .isEqualTo(replicaTableLocation.getParentFile().getAbsolutePath());
    assertThat(context.getCircusTrainConfigLocation()).isNull();
  }

  @Test
  public void createContextForNotExistingReplicaTable() throws Exception {
    reset(replicaMetaStoreClient);
    when(replicaMetaStoreClient.getTable(DATABASE, TABLE)).thenThrow(NoSuchObjectException.class);
    Database replicaDatabase = mock(Database.class);
    when(replicaDatabase.getLocationUri()).thenReturn(REPLICA_DATABASE_LOCATION);
    when(replicaMetaStoreClient.getDatabase(DATABASE)).thenReturn(replicaDatabase);
    Context context = factory.createContext(event);
    verify(marshaller).marshall(eq(context.getConfigLocation()), circusTrainConfigCaptor.capture());
    CircusTrainConfig circusTrainConfig = circusTrainConfigCaptor.getValue();
    assertThat(circusTrainConfig.getTableReplications()).hasSize(1);
    TableReplication replication = circusTrainConfig.getTableReplications().get(0);
    assertTableReplication(replication);
    assertThat(replication.getReplicationMode()).isSameAs(event.getReplicationMode());
    assertThat(replication.getReplicaTable().getTableLocation()).isEqualTo(REPLICA_DATABASE_LOCATION + "/" + TABLE);
    assertThat(context.getCircusTrainConfigLocation()).isNull();
  }

  @Test
  public void ctConfigLocationGetsAddedToTheContextWhenProvided() {
    conf.set(CT_CONFIG.key(), "ct-config.yml");
    when(event.getPartitionColumns()).thenReturn(Arrays.asList("s", "i"));
    when(event.getPartitionValues()).thenReturn(Arrays.asList(Arrays.asList("a", "1"), Arrays.asList("b", "2")));
    Context context = factory.createContext(event);
    assertThat(context.getWorkspace()).matches(actualWorkspacePattern);
    assertThat(context.getConfigLocation()).matches(actualWorkspacePattern + "/replication.yml");
    verify(marshaller).marshall(eq(context.getConfigLocation()), circusTrainConfigCaptor.capture());
    CircusTrainConfig circusTrainConfig = circusTrainConfigCaptor.getValue();
    assertCircusTrainConfig(circusTrainConfig);
    TableReplication replication = circusTrainConfig.getTableReplications().get(0);
    assertTableReplication(replication);
    assertThat(replication.getReplicationMode()).isSameAs(event.getReplicationMode());
    assertThat(replication.getSourceTable().getPartitionFilter()).isEqualTo("(s='a' AND i='1') OR (s='b' AND i='2')");
    assertThat(replication.getReplicaTable().getTableLocation())
        .isEqualTo(replicaTableLocation.getParentFile().getAbsolutePath());
    assertThat(context.getCircusTrainConfigLocation()).isEqualTo("ct-config.yml");
  }

  @Test
  public void replicationModeIsMetadataUpdateWhenProvidedAsSuchInTheEvent() {
    when(event.getReplicationMode()).thenReturn(ReplicationMode.METADATA_UPDATE);
    Context context = factory.createContext(event);
    assertThat(context.getWorkspace()).matches(actualWorkspacePattern);
    assertThat(context.getConfigLocation()).matches(actualWorkspacePattern + "/replication.yml");
    verify(marshaller).marshall(eq(context.getConfigLocation()), circusTrainConfigCaptor.capture());
    CircusTrainConfig circusTrainConfig = circusTrainConfigCaptor.getValue();
    assertCircusTrainConfig(circusTrainConfig);
    TableReplication replication = circusTrainConfig.getTableReplications().get(0);
    assertTableReplication(replication);

    assertThat(replication.getReplicationMode()).isSameAs(event.getReplicationMode());
    assertThat(replication.getSourceTable().getPartitionFilter()).isBlank();
    assertThat(replication.getReplicaTable().getTableLocation())
        .isEqualTo(replicaTableLocation.getParentFile().getAbsolutePath());
    assertThat(context.getCircusTrainConfigLocation()).isNull();
  }

  @Test(expected = ShuntingYardException.class)
  public void failIfReplicaDatabseDoesNotExist() throws Exception {
    reset(replicaMetaStoreClient);
    when(replicaMetaStoreClient.getTable(DATABASE, TABLE)).thenThrow(NoSuchObjectException.class);
    when(replicaMetaStoreClient.getDatabase(DATABASE)).thenThrow(TException.class);
    factory.createContext(event);
  }

  private void assertCircusTrainConfig(CircusTrainConfig circusTrainConfig) {
    assertThat(circusTrainConfig.getSourceCatalog().getHiveMetastoreUris()).isEqualTo(SOURCE_METASTORE_URIS);
    assertThat(circusTrainConfig.getReplicaCatalog().getHiveMetastoreUris()).isEqualTo(REPLICA_METASTORE_URIS);
    assertThat(circusTrainConfig.getTableReplications()).hasSize(1);
  }

  private void assertTableReplication(TableReplication replication) {
    assertThat(replication.getSourceTable().getDatabaseName()).isEqualTo(DATABASE);
    assertThat(replication.getSourceTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getReplicaTable().getDatabaseName()).isEqualTo(DATABASE);
    assertThat(replication.getReplicaTable().getTableName()).isEqualTo(TABLE);
  }

}
