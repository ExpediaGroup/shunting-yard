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
package com.hotels.shunting.yard.replicator.exec.receiver;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import static com.hotels.shunting.yard.replicator.exec.app.ConfigurationVariables.WORKSPACE;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
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

import com.hotels.bdp.circustrain.core.conf.ReplicationMode;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
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

  public @Rule TemporaryFolder tmp = new TemporaryFolder();

  private @Mock CloseableMetaStoreClient replicaMetaStoreClient;
  private @Mock Marshaller marshaller;
  private @Mock SerializableListenerEvent event;
  private @Mock Table sourceTable;
  private @Mock Partition sourcePartitionA, sourcePartitionB;
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
    actualWorkspacePattern = workspaceDir.getAbsolutePath() + "/ON_CREATE_TABLE_\\d{8}T\\d{9}";
    conf.set(WORKSPACE.key(), workspaceDir.getAbsolutePath());
    conf.set(METASTOREURIS.varname, REPLICA_METASTORE_URIS);

    when(event.getParameters()).thenReturn(eventParameters);
    when(event.getEventType()).thenReturn(EventType.ON_CREATE_TABLE);
    when(eventParameters.get(METASTOREURIS.varname)).thenReturn(SOURCE_METASTORE_URIS);

    when(sourceTable.getDbName()).thenReturn(DATABASE);
    when(sourceTable.getTableName()).thenReturn(TABLE);

    replicaTableLocation = tmp.newFolder(CT_EVENT_ID);
    when(replicaStorageDescriptor.getLocation()).thenReturn(replicaTableLocation.getAbsolutePath());

    when(replicaTable.getSd()).thenReturn(replicaStorageDescriptor);

    when(replicaMetaStoreClient.getTable(DATABASE, TABLE)).thenReturn(replicaTable);

    factory = new ContextFactory(conf, replicaMetaStoreClient, marshaller);
  }

  @Test
  public void createContextForUnpartitionedTable() {
    Context context = factory.createContext(event, sourceTable);
    assertThat(context.getWorkspace()).matches(actualWorkspacePattern);
    assertThat(context.getConfigLocation()).matches(actualWorkspacePattern + "/replication.yml");
    verify(marshaller).marshall(eq(context.getConfigLocation()), circusTrainConfigCaptor.capture());
    CircusTrainConfig circusTrainConfig = circusTrainConfigCaptor.getValue();
    assertThat(circusTrainConfig.getSourceCatalog().getHiveMetastoreUris()).isEqualTo(SOURCE_METASTORE_URIS);
    assertThat(circusTrainConfig.getReplicaCatalog().getHiveMetastoreUris()).isEqualTo(REPLICA_METASTORE_URIS);
    assertThat(circusTrainConfig.getTableReplications()).hasSize(1);
    TableReplication replication = circusTrainConfig.getTableReplications().get(0);
    assertThat(replication.getReplicationMode()).isSameAs(ReplicationMode.FULL);
    assertThat(replication.getSourceTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getSourceTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getSourceTable().getPartitionFilter()).isBlank();
    assertThat(replication.getReplicaTable().getDatabaseName()).isEqualTo(DATABASE);
    assertThat(replication.getReplicaTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getReplicaTable().getTableLocation())
        .isEqualTo(replicaTableLocation.getParentFile().getAbsolutePath());
  }

  @Test
  public void createContextForPartitionedTable() {
    when(sourceTable.getPartitionKeys())
        .thenReturn(Arrays.asList(new FieldSchema("s", "string", null), new FieldSchema("i", "integer", null)));
    when(sourcePartitionA.getValues()).thenReturn(Arrays.asList("a", "1"));
    when(sourcePartitionB.getValues()).thenReturn(Arrays.asList("b", "2"));
    Context context = factory.createContext(event, sourceTable, Arrays.asList(sourcePartitionA, sourcePartitionB));
    assertThat(context.getWorkspace()).matches(actualWorkspacePattern);
    assertThat(context.getConfigLocation()).matches(actualWorkspacePattern + "/replication.yml");
    verify(marshaller).marshall(eq(context.getConfigLocation()), circusTrainConfigCaptor.capture());
    CircusTrainConfig circusTrainConfig = circusTrainConfigCaptor.getValue();
    assertThat(circusTrainConfig.getSourceCatalog().getHiveMetastoreUris()).isEqualTo(SOURCE_METASTORE_URIS);
    assertThat(circusTrainConfig.getReplicaCatalog().getHiveMetastoreUris()).isEqualTo(REPLICA_METASTORE_URIS);
    assertThat(circusTrainConfig.getTableReplications()).hasSize(1);
    TableReplication replication = circusTrainConfig.getTableReplications().get(0);
    assertThat(replication.getReplicationMode()).isSameAs(ReplicationMode.FULL);
    assertThat(replication.getSourceTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getSourceTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getSourceTable().getPartitionFilter()).isEqualTo("(s='a' AND i=1) OR (s='b' AND i=2)");
    assertThat(replication.getReplicaTable().getDatabaseName()).isEqualTo(DATABASE);
    assertThat(replication.getReplicaTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getReplicaTable().getTableLocation())
        .isEqualTo(replicaTableLocation.getParentFile().getAbsolutePath());
  }

  @Test
  public void createContextForNotExistingReplicaTable() throws Exception {
    reset(replicaMetaStoreClient);
    when(replicaMetaStoreClient.getTable(DATABASE, TABLE)).thenThrow(NoSuchObjectException.class);
    Database replicaDatabase = mock(Database.class);
    when(replicaDatabase.getLocationUri()).thenReturn(REPLICA_DATABASE_LOCATION);
    when(replicaMetaStoreClient.getDatabase(DATABASE)).thenReturn(replicaDatabase);
    Context context = factory.createContext(event, sourceTable);
    verify(marshaller).marshall(eq(context.getConfigLocation()), circusTrainConfigCaptor.capture());
    CircusTrainConfig circusTrainConfig = circusTrainConfigCaptor.getValue();
    TableReplication replication = circusTrainConfig.getTableReplications().get(0);
    assertThat(replication.getSourceTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getSourceTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getReplicaTable().getDatabaseName()).isEqualTo(DATABASE);
    assertThat(replication.getReplicaTable().getTableName()).isEqualTo(TABLE);
    assertThat(replication.getReplicaTable().getTableLocation()).isEqualTo(REPLICA_DATABASE_LOCATION + "/" + TABLE);
  }

  @Test(expected = ShuntingYardException.class)
  public void failIfReplicaDatabseDoesNotExist() throws Exception {
    reset(replicaMetaStoreClient);
    when(replicaMetaStoreClient.getTable(DATABASE, TABLE)).thenThrow(NoSuchObjectException.class);
    when(replicaMetaStoreClient.getDatabase(DATABASE)).thenThrow(TException.class);
    factory.createContext(event, sourceTable);
  }

}
