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

import static org.apache.hadoop.hive.common.StatsSetupConst.CASCADE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.shunting.yard.common.event.EventType.ON_ADD_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_ALTER_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_ALTER_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.ON_CREATE_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.ON_INSERT;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ImmutableMap;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.receiver.thrift.ThriftListenerUtils;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.launcher.CircusTrainRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ThriftListenerUtils.class)
public class CircusTrainReplicationMetaStoreEventListenerTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";

  public static @Rule MockitoJUnit mockito = new MockitoJUnit();

  private @Mock CloseableMetaStoreClient metaStoreClient;
  private @Mock ContextFactory contextFactory;
  private @Mock CircusTrainRunner circusTrainRunner;
  private @Mock Context context;
  private @Mock Table table;
  private @Mock Partition partition;

  private @Captor ArgumentCaptor<EnvironmentContext> environmentContextCaptor;

  private final Map<String, String> tableParameters = new HashMap<>();
  private CircusTrainReplicationMetaStoreEventListener listener;

  @Before
  public void init() throws Exception {
    mockStatic(ThriftListenerUtils.class);
    tableParameters.put(REPLICATION_EVENT.parameterName(), "cte-123");
    when(table.getDbName()).thenReturn(DATABASE);
    when(table.getTableName()).thenReturn(TABLE);
    when(table.getParameters()).thenReturn(tableParameters);
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenReturn(table);
    when(partition.getDbName()).thenReturn(DATABASE);
    when(partition.getTableName()).thenReturn(TABLE);
    listener = new CircusTrainReplicationMetaStoreEventListener(metaStoreClient, contextFactory, circusTrainRunner);
  }

  @Test
  public void canReplicateTable() {
    assertThat(listener.canReplicate(DATABASE, TABLE)).isTrue();
  }

  @Test
  public void canReplicateTableIfTableDoesNotExists() throws Exception {
    reset(metaStoreClient);
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(NoSuchObjectException.class);
    assertThat(listener.canReplicate(DATABASE, TABLE)).isTrue();
  }

  @Test
  public void cannotReplicateTable() {
    tableParameters.clear();
    listener.canReplicate(DATABASE, TABLE);
  }

  @Test(expected = ShuntingYardException.class)
  public void cannotReplicateTableIfExceptionIsThrown() throws Exception {
    reset(metaStoreClient);
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(TException.class);
    listener.canReplicate(DATABASE, TABLE);
  }

  @Test
  public void onCreateTableReplicates() {
    MetaStoreEvent event = mockEvent(ON_CREATE_TABLE);
    when(contextFactory.createContext(event)).thenReturn(context);
    listener.onEvent(event);
    verify(circusTrainRunner).run(context);
  }

  @Test
  public void onCreateTableDoesNotReplicate() {
    tableParameters.clear();
    MetaStoreEvent event = mockEvent(ON_CREATE_TABLE);
    listener.onEvent(event);
    verifyZeroInteractions(contextFactory);
    verifyZeroInteractions(circusTrainRunner);
  }

  @Test
  public void onDropTableDeletesTable() throws Exception {
    MetaStoreEvent event = mockEvent(ON_DROP_TABLE);
    listener.onEvent(event);
    verify(metaStoreClient).dropTable(DATABASE, TABLE, false, true);
  }

  @Test
  public void onDropTableDeletesTableAndData() throws Exception {
    MetaStoreEvent event = mockEvent(ON_DROP_TABLE);
    when(event.isDeleteData()).thenReturn(true);
    listener.onEvent(event);
    verify(metaStoreClient).dropTable(DATABASE, TABLE, true, true);
  }

  @Test
  public void onDropTableDoesNotDeleteTable() throws Exception {
    tableParameters.clear();
    MetaStoreEvent event = mockEvent(ON_DROP_TABLE);
    listener.onEvent(event);
    verify(metaStoreClient, never()).dropTable(anyString(), anyString(), anyBoolean(), anyBoolean());
  }

  @Test
  public void onAlterTableReplicates() throws Exception {
    final String newTableName = "new_tbl";
    Table newTable = mock(Table.class);
    when(newTable.getDbName()).thenReturn(DATABASE);
    when(newTable.getTableName()).thenReturn(newTableName);
    when(newTable.getParameters()).thenReturn(tableParameters);
    when(metaStoreClient.getTable(DATABASE, newTableName)).thenReturn(newTable);
    Map<String, String> envContextProperties = ImmutableMap.of("key", "value", CASCADE, "true");
    EnvironmentContext environment = mock(EnvironmentContext.class);
    when(environment.getProperties()).thenReturn(envContextProperties);
    MetaStoreEvent event = mockEvent(ON_ALTER_TABLE);
    when(event.getDatabaseName()).thenReturn(DATABASE);
    when(event.getTableName()).thenReturn(newTableName);
    when(event.getEnvironmentContext()).thenReturn(envContextProperties);
    when(event.isCascade()).thenReturn(true);
    when(contextFactory.createContext(event)).thenReturn(context);
    listener.onEvent(event);
    verify(circusTrainRunner).run(context);
    verify(metaStoreClient).alter_table_with_environmentContext(eq(DATABASE), eq(newTableName), same(newTable),
        environmentContextCaptor.capture());
    assertThat(environmentContextCaptor.getValue().getProperties()).isEqualTo(envContextProperties);
  }

  @Test
  public void onAlterTableReplicatesButCannotCascadeUpdates() throws Exception {
    final String newTableName = "new_tbl";
    Table newTable = mock(Table.class);
    when(newTable.getDbName()).thenReturn(DATABASE);
    when(newTable.getTableName()).thenReturn(newTableName);
    when(newTable.getParameters()).thenReturn(tableParameters);
    when(metaStoreClient.getTable(DATABASE, newTableName)).thenReturn(newTable);
    Map<String, String> envContextProperties = ImmutableMap.of("key", "value", CASCADE, "true");
    EnvironmentContext environment = mock(EnvironmentContext.class);
    when(environment.getProperties()).thenReturn(envContextProperties);
    MetaStoreEvent event = mockEvent(ON_ALTER_TABLE);
    when(event.getDatabaseName()).thenReturn(DATABASE);
    when(event.getTableName()).thenReturn(newTableName);
    when(event.getEnvironmentContext()).thenReturn(envContextProperties);
    when(event.isCascade()).thenReturn(true);
    when(contextFactory.createContext(event)).thenReturn(context);
    doThrow(RuntimeException.class).when(metaStoreClient).alter_table_with_environmentContext(eq(DATABASE),
        eq(newTableName), same(newTable), any());
    listener.onEvent(event);
    verify(circusTrainRunner).run(context);
    verify(metaStoreClient).alter_table_with_environmentContext(eq(DATABASE), eq(newTableName), same(newTable),
        environmentContextCaptor.capture());
    assertThat(environmentContextCaptor.getValue().getProperties()).isEqualTo(envContextProperties);
  }

  @Test
  public void onAlterTableDoesNotReplicate() throws Exception {
    tableParameters.clear();
    MetaStoreEvent event = mockEvent(ON_ALTER_TABLE);
    listener.onEvent(event);
    verifyZeroInteractions(contextFactory);
    verifyZeroInteractions(circusTrainRunner);
    verify(metaStoreClient, never()).alter_table_with_environmentContext(anyString(), anyString(), any(Table.class),
        any(EnvironmentContext.class));
  }

  @Test
  public void onAddPartitionReplicates() {
    MetaStoreEvent event = mockEvent(ON_ADD_PARTITION);
    when(contextFactory.createContext(event)).thenReturn(context);
    listener.onEvent(event);
    verify(circusTrainRunner).run(context);
  }

  @Test
  public void onAddPartitionDoesNotReplicate() {
    tableParameters.clear();
    MetaStoreEvent event = mockEvent(ON_ADD_PARTITION);
    when(contextFactory.createContext(same(event))).thenReturn(context);
    listener.onEvent(event);
    verifyZeroInteractions(contextFactory);
    verifyZeroInteractions(circusTrainRunner);
  }

  @Test
  public void onDropPartitionDeletesPartition() throws Exception {
    List<String> partitions = Arrays.asList("a", "b");
    MetaStoreEvent event = mockEvent(ON_DROP_PARTITION);
    when(event.getPartitionValues()).thenReturn(Arrays.asList(partitions));
    listener.onEvent(event);
    verify(metaStoreClient).dropPartition(DATABASE, TABLE, partitions, false);
  }

  @Test
  public void onDropPartitionDeletesPartitionAndData() throws Exception {
    List<String> partitions = Arrays.asList("a", "b");
    MetaStoreEvent event = mockEvent(ON_DROP_PARTITION);
    when(event.getPartitionValues()).thenReturn(Arrays.asList(partitions));
    when(event.isDeleteData()).thenReturn(true);
    listener.onEvent(event);
    verify(metaStoreClient).dropPartition(DATABASE, TABLE, partitions, true);
  }

  @Test
  public void onDropPartitionDoesNotDeletePartition() throws Exception {
    tableParameters.clear();
    MetaStoreEvent event = mockEvent(ON_DROP_PARTITION);
    listener.onEvent(event);
    verify(metaStoreClient, never()).dropPartitions(anyString(), anyString(), any(List.class), anyBoolean(),
        anyBoolean(), anyBoolean());
  }

  @Test
  public void onAlterPartitionReplicates() {
    MetaStoreEvent event = mockEvent(ON_ALTER_PARTITION);
    when(contextFactory.createContext(same(event))).thenReturn(context);
    listener.onEvent(event);
    verify(circusTrainRunner).run(context);
  }

  @Test
  public void onAlterPartitionDoesNotReplicate() {
    tableParameters.clear();
    MetaStoreEvent event = mockEvent(ON_ALTER_PARTITION);
    listener.onEvent(event);
    verifyZeroInteractions(contextFactory);
    verifyZeroInteractions(circusTrainRunner);
  }

  @Test
  public void onInsertReplicates() {
    MetaStoreEvent event = mockEvent(ON_INSERT);
    when(contextFactory.createContext(same(event))).thenReturn(context);
    listener.onEvent(event);
    verify(circusTrainRunner).run(context);
  }

  @Test
  public void onInsertDoesNotReplicate() {
    tableParameters.clear();
    MetaStoreEvent event = mockEvent(ON_INSERT);
    listener.onEvent(event);
    verifyZeroInteractions(contextFactory);
    verifyZeroInteractions(circusTrainRunner);
  }

  private static MetaStoreEvent mockEvent(EventType eventType) {
    MetaStoreEvent event = mock(MetaStoreEvent.class);
    when(event.getEventType()).thenReturn(eventType);
    when(event.getDatabaseName()).thenReturn(DATABASE);
    when(event.getTableName()).thenReturn(TABLE);
    return event;
  }

}
