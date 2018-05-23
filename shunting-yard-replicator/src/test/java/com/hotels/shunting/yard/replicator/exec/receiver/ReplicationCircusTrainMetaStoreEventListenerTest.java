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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.expedia.hdw.common.hive.metastore.CloseableMetaStoreClient;

import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.event.SerializableAddPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterTableEvent;
import com.hotels.shunting.yard.common.event.SerializableCreateTableEvent;
import com.hotels.shunting.yard.common.event.SerializableDropPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropTableEvent;
import com.hotels.shunting.yard.common.event.SerializableInsertEvent;
import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.receiver.thrift.ThriftListenerUtils;
import com.hotels.shunting.yard.replicator.exec.launcher.CircusTrainRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ThriftListenerUtils.class)
public class ReplicationCircusTrainMetaStoreEventListenerTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";

  public static @Rule MockitoJUnit mockito = new MockitoJUnit();

  private @Mock CloseableMetaStoreClient metaStoreClient;
  private @Mock ContextFactory contextFactory;
  private @Mock CircusTrainRunner circusTrainRunner;
  private @Mock Context context;
  private @Mock Table table;
  private @Mock Partition partition;
  private @Mock List<ObjectPair<Integer, byte[]>> objectPairs;

  private final Map<String, String> tableParameters = new HashMap<>();
  private ReplicationCircusTrainMetaStoreEventListener listener;

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
    listener = new ReplicationCircusTrainMetaStoreEventListener(metaStoreClient, contextFactory, circusTrainRunner);
  }

  @Test
  public void canReplicateTable() {
    assertThat(listener.canReplicate(table)).isTrue();
  }

  @Test
  public void canReplicateTableIfTableDoesNotExists() throws Exception {
    reset(metaStoreClient);
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(NoSuchObjectException.class);
    assertThat(listener.canReplicate(table)).isTrue();
  }

  @Test
  public void cannotReplicateTable() {
    tableParameters.clear();
    listener.canReplicate(table);
  }

  @Test(expected = ShuntingYardException.class)
  public void cannotReplicateTableIfExceptionIsThrown() throws Exception {
    reset(metaStoreClient);
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(TException.class);
    listener.canReplicate(table);
  }

  @Test
  public void canReplicatePartition() {
    assertThat(listener.canReplicate(partition)).isTrue();
  }

  @Test
  public void canReplicatePartitionIfTableDoesNotExists() throws Exception {
    reset(metaStoreClient);
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(NoSuchObjectException.class);
    assertThat(listener.canReplicate(partition)).isTrue();
  }

  @Test
  public void cannotReplicatePartition() {
    tableParameters.clear();
    assertThat(listener.canReplicate(partition)).isFalse();
  }

  @Test(expected = ShuntingYardException.class)
  public void cannotReplicatePartitionIfExceptionIsThrown() throws Exception {
    reset(metaStoreClient);
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(TException.class);
    listener.canReplicate(partition);
  }

  @Test
  public void onCreateTableReplicates() {
    SerializableCreateTableEvent event = mockEvent(SerializableCreateTableEvent.class);
    when(event.getTable()).thenReturn(table);
    when(contextFactory.createContext(event, table)).thenReturn(context);
    listener.onCreateTable(event);
    verify(circusTrainRunner).run(context);
  }

  @Test
  public void onCreateTableDoesNotReplicate() {
    tableParameters.clear();
    SerializableCreateTableEvent event = mockEvent(SerializableCreateTableEvent.class);
    when(event.getTable()).thenReturn(table);
    listener.onCreateTable(event);
    verifyZeroInteractions(contextFactory);
    verifyZeroInteractions(circusTrainRunner);
  }

  @Test
  public void onDropTableDeletesTable() throws Exception {
    SerializableDropTableEvent event = mockEvent(SerializableDropTableEvent.class);
    when(event.getTable()).thenReturn(table);
    listener.onDropTable(event);
    verify(metaStoreClient).dropTable(DATABASE, TABLE, false, true);
  }

  @Test
  public void onDropTableDeletesTableAndData() throws Exception {
    SerializableDropTableEvent event = mockEvent(SerializableDropTableEvent.class);
    when(event.getTable()).thenReturn(table);
    when(event.getDeleteData()).thenReturn(true);
    listener.onDropTable(event);
    verify(metaStoreClient).dropTable(DATABASE, TABLE, true, true);
  }

  @Test
  public void onDropTableDoesNotDeleteTable() throws Exception {
    tableParameters.clear();
    SerializableDropTableEvent event = mockEvent(SerializableDropTableEvent.class);
    when(event.getTable()).thenReturn(table);
    listener.onDropTable(event);
    verify(metaStoreClient, never()).dropTable(anyString(), anyString(), anyBoolean(), anyBoolean());
  }

  @Test
  public void onAlterTableReplicates() throws Exception {
    Table newTable = mock(Table.class);
    when(newTable.getDbName()).thenReturn(DATABASE);
    when(newTable.getTableName()).thenReturn("new_tbl");
    when(metaStoreClient.getTable(DATABASE, "new_tbl")).thenReturn(newTable);
    EnvironmentContext environment = mock(EnvironmentContext.class);
    SerializableAlterTableEvent event = mockEvent(SerializableAlterTableEvent.class);
    when(event.getOldTable()).thenReturn(table);
    when(event.getNewTable()).thenReturn(newTable);
    when(event.getEnvironmentContext()).thenReturn(environment);
    when(contextFactory.createContext(event, newTable)).thenReturn(context);
    listener.onAlterTable(event);
    verify(circusTrainRunner).run(context);
    verify(metaStoreClient).alter_table_with_environmentContext(DATABASE, "new_tbl", newTable, environment);
  }

  @Test
  public void onAlterTableDoesNotReplicate() throws Exception {
    tableParameters.clear();
    SerializableAlterTableEvent event = mockEvent(SerializableAlterTableEvent.class);
    when(event.getOldTable()).thenReturn(table);
    listener.onAlterTable(event);
    verifyZeroInteractions(contextFactory);
    verifyZeroInteractions(circusTrainRunner);
    verify(metaStoreClient, never()).alter_table_with_environmentContext(anyString(), anyString(), any(Table.class),
        any(EnvironmentContext.class));
  }

  @Test
  public void onAddPartitionReplicates() {
    List<Partition> partitions = Arrays.asList(partition);
    SerializableAddPartitionEvent event = mockEvent(SerializableAddPartitionEvent.class);
    when(event.getTable()).thenReturn(table);
    when(event.getPartitions()).thenReturn(partitions);
    when(contextFactory.createContext(event, table, partitions)).thenReturn(context);
    listener.onAddPartition(event);
    verify(circusTrainRunner).run(context);
  }

  @Test
  public void onAddPartitionDoesNotReplicate() {
    tableParameters.clear();
    SerializableAddPartitionEvent event = mockEvent(SerializableAddPartitionEvent.class);
    when(event.getTable()).thenReturn(table);
    when(contextFactory.createContext(same(event), same(table), eq(Arrays.asList(partition)))).thenReturn(context);
    listener.onAddPartition(event);
    verifyZeroInteractions(contextFactory);
    verifyZeroInteractions(circusTrainRunner);
  }

  @Test
  public void onDropPartitionDeletesPartition() throws Exception {
    List<Partition> partitions = Arrays.asList(partition);
    when(ThriftListenerUtils.toObjectPairs(table, partitions)).thenReturn(objectPairs);
    SerializableDropPartitionEvent event = mockEvent(SerializableDropPartitionEvent.class);
    when(event.getTable()).thenReturn(table);
    when(event.getPartitions()).thenReturn(partitions);
    listener.onDropPartition(event);
    verify(metaStoreClient).dropPartitions(DATABASE, TABLE, objectPairs, false, true, false);
  }

  @Test
  public void onDropPartitionDeletesPartitionAndData() throws Exception {
    List<Partition> partitions = Arrays.asList(partition);
    when(ThriftListenerUtils.toObjectPairs(table, partitions)).thenReturn(objectPairs);
    SerializableDropPartitionEvent event = mockEvent(SerializableDropPartitionEvent.class);
    when(event.getTable()).thenReturn(table);
    when(event.getPartitions()).thenReturn(partitions);
    when(event.getDeleteData()).thenReturn(true);
    listener.onDropPartition(event);
    verify(metaStoreClient).dropPartitions(DATABASE, TABLE, objectPairs, true, true, false);
  }

  @Test
  public void onDropPartitionDoesNotDeletePartition() throws Exception {
    tableParameters.clear();
    SerializableDropPartitionEvent event = mockEvent(SerializableDropPartitionEvent.class);
    when(event.getTable()).thenReturn(table);
    listener.onDropPartition(event);
    verify(metaStoreClient, never()).dropPartitions(anyString(), anyString(), any(List.class), anyBoolean(),
        anyBoolean(), anyBoolean());
  }

  @Test
  public void onAlterPartitionReplicates() {
    SerializableAlterPartitionEvent event = mockEvent(SerializableAlterPartitionEvent.class);
    when(event.getTable()).thenReturn(table);
    when(event.getNewPartition()).thenReturn(partition);
    when(contextFactory.createContext(same(event), same(table), eq(Arrays.asList(partition)))).thenReturn(context);
    listener.onAlterPartition(event);
    verify(circusTrainRunner).run(context);
  }

  @Test
  public void onAlterPartitionDoesNotReplicate() {
    tableParameters.clear();
    SerializableAlterPartitionEvent event = mockEvent(SerializableAlterPartitionEvent.class);
    when(event.getTable()).thenReturn(table);
    listener.onAlterPartition(event);
    verifyZeroInteractions(contextFactory);
    verifyZeroInteractions(circusTrainRunner);
  }

  @Test
  public void onInsert() {
    SerializableInsertEvent event = mockEvent(SerializableInsertEvent.class);
    listener.onInsert(event);
    verifyNoMoreInteractions(metaStoreClient);
    verifyNoMoreInteractions(circusTrainRunner);
  }

  private static <T extends SerializableListenerEvent> T mockEvent(Class<T> clazz) {
    T event = mock(clazz);
    when(event.getDatabaseName()).thenReturn(DATABASE);
    when(event.getTableName()).thenReturn(TABLE);
    return event;
  }

}
