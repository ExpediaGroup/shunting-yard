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
package com.hotels.shunting.yard.common.receiver.thrift;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.event.SerializableAddPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterTableEvent;
import com.hotels.shunting.yard.common.event.SerializableCreateTableEvent;
import com.hotels.shunting.yard.common.event.SerializableDropPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropTableEvent;
import com.hotels.shunting.yard.common.event.SerializableInsertEvent;
import com.hotels.shunting.yard.common.event.SerializableListenerEvent;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ThriftListenerUtils.class)
public class ThriftShuntingYardMetaStoreEventListenerTest {

  private static final String REPLICATION_EVENT = "com.hotels.bdp.circustrain.replication.event";
  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final boolean DELETE_DATA = false;
  private static final boolean EXISTS = true;

  private @Mock EnvironmentContext eventContext;
  private @Mock CloseableMetaStoreClient metaStoreClient;
  private @Mock Table sourceTable;
  private @Mock Table targetTable;
  private @Mock Partition sourcePartition;
  private @Mock Map<String, String> sourceTableParameters;
  private @Mock Map<String, String> targetTableParameters;
  private @Mock Map<String, String> sourcePartitionParameters;

  private @Captor ArgumentCaptor<String> eventKeyCaptor;

  private ThriftShuntingYardMetaStoreEventListener listener;

  private <T extends SerializableListenerEvent> T mockEvent(Class<T> clazz) {
    T event = mock(clazz);
    when(event.getEnvironmentContext()).thenReturn(eventContext);
    return event;
  }

  @Before
  public void init() throws Exception {
    mockStatic(ThriftListenerUtils.class);
    when(sourceTable.getDbName()).thenReturn(DATABASE);
    when(sourceTable.getTableName()).thenReturn(TABLE);
    when(sourceTable.getParameters()).thenReturn(sourceTableParameters);

    when(sourcePartition.getDbName()).thenReturn(DATABASE);
    when(sourcePartition.getTableName()).thenReturn(TABLE);
    when(sourcePartition.getParameters()).thenReturn(sourcePartitionParameters);

    when(targetTable.getDbName()).thenReturn(DATABASE);
    when(targetTable.getTableName()).thenReturn(TABLE);
    when(targetTable.getParameters()).thenReturn(targetTableParameters);

    when(metaStoreClient.getTable(DATABASE, TABLE)).thenReturn(targetTable);
    listener = new ThriftShuntingYardMetaStoreEventListener(metaStoreClient);
  }

  @Test
  public void canReplicateTable() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    assertThat(listener.canReplicate(sourceTable)).isTrue();
  }

  @Test
  public void canReplicateTableIfTableDoesNotExist() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(NoSuchObjectException.class);
    assertThat(listener.canReplicate(sourceTable)).isTrue();
  }

  @Test(expected = ShuntingYardException.class)
  public void cannotReplicateTableMetastoreClientThrowsException() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(TException.class);
    listener.canReplicate(sourceTable);
  }

  @Test
  public void cannotReplicateTable() throws Exception {
    assertThat(listener.canReplicate(sourceTable)).isFalse();
  }

  @Test
  public void canReplicatePartition() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    assertThat(listener.canReplicate(sourcePartition)).isTrue();
  }

  @Test
  public void canReplicatePartitionIfTableDoesNotExist() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(NoSuchObjectException.class);
    assertThat(listener.canReplicate(sourcePartition)).isTrue();
  }

  @Test(expected = ShuntingYardException.class)
  public void cannotReplicatePartitionIfMetastoreClientThrowsException() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenThrow(TException.class);
    listener.canReplicate(sourcePartition);
  }

  @Test
  public void cannotReplicatePartition() throws Exception {
    assertThat(listener.canReplicate(sourcePartition)).isFalse();
  }

  @Test
  public void onCreateTable() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    SerializableCreateTableEvent event = mockEvent(SerializableCreateTableEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    listener.onCreateTable(event);
    verify(metaStoreClient).createTable(sourceTable);
    verify(sourceTableParameters).put(eventKeyCaptor.capture(), anyString());
    assertThat(eventKeyCaptor.getValue()).isEqualTo(REPLICATION_EVENT);
  }

  @Test(expected = ShuntingYardException.class)
  public void onCreateTableFails() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    SerializableCreateTableEvent event = mockEvent(SerializableCreateTableEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    doThrow(TException.class).when(metaStoreClient).createTable(sourceTable);
    listener.onCreateTable(event);
  }

  @Test
  public void onCreateTableSkipsReplication() throws Exception {
    SerializableCreateTableEvent event = mockEvent(SerializableCreateTableEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    listener.onCreateTable(event);
    verify(metaStoreClient).getTable(eq(DATABASE), eq(TABLE));
    verifyNoMoreInteractions(metaStoreClient);
  }

  @Test
  public void onDropTable() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    SerializableDropTableEvent event = mockEvent(SerializableDropTableEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    listener.onDropTable(event);
    verify(metaStoreClient).dropTable(eq(DATABASE), eq(TABLE), eq(DELETE_DATA), eq(EXISTS));
  }

  @Test(expected = ShuntingYardException.class)
  public void onDropTableFails() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    SerializableDropTableEvent event = mockEvent(SerializableDropTableEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    doThrow(TException.class).when(metaStoreClient).dropTable(eq(DATABASE), eq(TABLE), eq(DELETE_DATA), eq(EXISTS));
    listener.onDropTable(event);
  }

  @Test
  public void onDropTableSkipsReplication() throws Exception {
    SerializableDropTableEvent event = mockEvent(SerializableDropTableEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    listener.onDropTable(event);
    verify(metaStoreClient).getTable(eq(DATABASE), eq(TABLE));
    verifyNoMoreInteractions(metaStoreClient);
  }

  @Test
  public void onAlterTable() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    final String oldDbName = "old_db";
    final String oldTableName = "old_tbl";
    Map<String, String> oldParameters = mock(Map.class);
    Table oldTable = mock(Table.class);
    when(oldTable.getDbName()).thenReturn(oldDbName);
    when(oldTable.getTableName()).thenReturn(oldTableName);
    when(oldTable.getParameters()).thenReturn(oldParameters);
    when(targetTable.getDbName()).thenReturn(oldDbName);
    when(targetTable.getTableName()).thenReturn(oldTableName);
    when(metaStoreClient.getTable(oldDbName, oldTableName)).thenReturn(targetTable);
    SerializableAlterTableEvent event = mockEvent(SerializableAlterTableEvent.class);
    when(event.getOldTable()).thenReturn(oldTable);
    when(event.getNewTable()).thenReturn(sourceTable);
    listener.onAlterTable(event);
    verify(metaStoreClient).alter_table_with_environmentContext(eq(oldDbName), eq(oldTableName), same(sourceTable),
        same(eventContext));
    verify(sourceTableParameters).put(eventKeyCaptor.capture(), anyString());
    assertThat(eventKeyCaptor.getValue()).isEqualTo(REPLICATION_EVENT);
  }

  @Test(expected = ShuntingYardException.class)
  public void onAlterTableFails() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    final String oldDbName = "old_db";
    final String oldTableName = "old_tbl";
    Map<String, String> oldParameters = mock(Map.class);
    Table oldTable = mock(Table.class);
    when(oldTable.getDbName()).thenReturn(oldDbName);
    when(oldTable.getTableName()).thenReturn(oldTableName);
    when(oldTable.getParameters()).thenReturn(oldParameters);
    when(targetTable.getDbName()).thenReturn(oldDbName);
    when(targetTable.getTableName()).thenReturn(oldTableName);
    when(metaStoreClient.getTable(oldDbName, oldTableName)).thenReturn(targetTable);
    SerializableAlterTableEvent event = mockEvent(SerializableAlterTableEvent.class);
    when(event.getOldTable()).thenReturn(oldTable);
    when(event.getNewTable()).thenReturn(sourceTable);
    doThrow(TException.class).when(metaStoreClient).alter_table_with_environmentContext(eq(oldDbName), eq(oldTableName),
        same(sourceTable), same(eventContext));
    listener.onAlterTable(event);
  }

  @Test
  public void onAlterTableSkipsReplication() throws Exception {
    final String oldDbName = "old_db";
    final String oldTableName = "old_tbl";
    Table oldTable = mock(Table.class);
    when(oldTable.getDbName()).thenReturn(oldDbName);
    when(oldTable.getTableName()).thenReturn(oldTableName);
    when(targetTable.getDbName()).thenReturn(oldDbName);
    when(targetTable.getTableName()).thenReturn(oldTableName);
    when(metaStoreClient.getTable(oldDbName, oldTableName)).thenReturn(targetTable);
    SerializableAlterTableEvent event = mockEvent(SerializableAlterTableEvent.class);
    when(event.getOldTable()).thenReturn(oldTable);
    listener.onAlterTable(event);
    verify(metaStoreClient).getTable(eq(oldDbName), eq(oldTableName));
    verifyNoMoreInteractions(metaStoreClient);
  }

  @Test
  public void onAddPartition() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    List<Partition> partitions = ImmutableList.of(sourcePartition);
    SerializableAddPartitionEvent event = mockEvent(SerializableAddPartitionEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    when(event.getPartitions()).thenReturn(partitions);
    listener.onAddPartition(event);
    verify(metaStoreClient).add_partitions(eq(partitions));
    verify(sourcePartitionParameters).put(eventKeyCaptor.capture(), anyString());
    assertThat(eventKeyCaptor.getValue()).isEqualTo(REPLICATION_EVENT);
  }

  @Test(expected = ShuntingYardException.class)
  public void onAddPartitionFails() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    List<Partition> partitions = ImmutableList.of(sourcePartition);
    SerializableAddPartitionEvent event = mockEvent(SerializableAddPartitionEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    when(event.getPartitions()).thenReturn(partitions);
    doThrow(TException.class).when(metaStoreClient).add_partitions(eq(partitions));
    listener.onAddPartition(event);
  }

  @Test
  public void onAddPartitionSkipsReplication() throws Exception {
    SerializableAddPartitionEvent event = mockEvent(SerializableAddPartitionEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    listener.onAddPartition(event);
    verify(metaStoreClient).getTable(eq(DATABASE), eq(TABLE));
    verifyNoMoreInteractions(metaStoreClient);
  }

  @Test
  public void onDropPartition() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    List<Partition> partitions = ImmutableList.of(sourcePartition);
    SerializableDropPartitionEvent event = mockEvent(SerializableDropPartitionEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    when(event.getPartitions()).thenReturn(partitions);
    when(ThriftListenerUtils.toObjectPairs(sourceTable, partitions))
        .thenReturn(ImmutableList.<ObjectPair<Integer, byte[]>> of());
    listener.onDropPartition(event);
    verify(metaStoreClient).dropPartitions(eq(DATABASE), eq(TABLE),
        eq(ImmutableList.<ObjectPair<Integer, byte[]>> of()), eq(DELETE_DATA), eq(EXISTS), eq(false));
  }

  @Test(expected = ShuntingYardException.class)
  public void onDropPartitionFails() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    List<Partition> partitions = ImmutableList.of(sourcePartition);
    SerializableDropPartitionEvent event = mockEvent(SerializableDropPartitionEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    when(event.getPartitions()).thenReturn(partitions);
    when(ThriftListenerUtils.toObjectPairs(sourceTable, partitions))
        .thenReturn(ImmutableList.<ObjectPair<Integer, byte[]>> of());
    doThrow(TException.class).when(metaStoreClient).dropPartitions(eq(DATABASE), eq(TABLE),
        eq(ImmutableList.<ObjectPair<Integer, byte[]>> of()), eq(DELETE_DATA), eq(EXISTS), eq(false));
    listener.onDropPartition(event);
  }

  @Test
  public void onDropPartitionSkipsReplication() throws Exception {
    SerializableDropPartitionEvent event = mockEvent(SerializableDropPartitionEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    listener.onDropPartition(event);
    verify(metaStoreClient).getTable(eq(DATABASE), eq(TABLE));
    verifyNoMoreInteractions(metaStoreClient);
  }

  @Test
  public void onAlterPartition() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    Partition oldPartition = mock(Partition.class);
    when(oldPartition.getDbName()).thenReturn(DATABASE);
    when(oldPartition.getTableName()).thenReturn(TABLE);
    SerializableAlterPartitionEvent event = mockEvent(SerializableAlterPartitionEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    when(event.getOldPartition()).thenReturn(oldPartition);
    when(event.getNewPartition()).thenReturn(sourcePartition);
    listener.onAlterPartition(event);
    verify(metaStoreClient).alter_partition(eq(DATABASE), eq(TABLE), same(sourcePartition), same(eventContext));
    verify(sourcePartitionParameters).put(eventKeyCaptor.capture(), anyString());
    assertThat(eventKeyCaptor.getValue()).isEqualTo(REPLICATION_EVENT);
  }

  @Test(expected = ShuntingYardException.class)
  public void onAlterPartitionFails() throws Exception {
    when(targetTableParameters.get(REPLICATION_EVENT)).thenReturn("123");
    Partition oldPartition = mock(Partition.class);
    when(oldPartition.getDbName()).thenReturn(DATABASE);
    when(oldPartition.getTableName()).thenReturn(TABLE);
    SerializableAlterPartitionEvent event = mockEvent(SerializableAlterPartitionEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    when(event.getOldPartition()).thenReturn(oldPartition);
    when(event.getNewPartition()).thenReturn(sourcePartition);
    doThrow(TException.class).when(metaStoreClient).alter_partition(eq(DATABASE), eq(TABLE), same(sourcePartition),
        same(eventContext));
    listener.onAlterPartition(event);
  }

  @Test
  public void onAlterPartitionSkipsReplication() throws Exception {
    SerializableAlterPartitionEvent event = mockEvent(SerializableAlterPartitionEvent.class);
    when(event.getTable()).thenReturn(sourceTable);
    listener.onAlterPartition(event);
    verify(metaStoreClient).getTable(eq(DATABASE), eq(TABLE));
    verifyNoMoreInteractions(metaStoreClient);
  }

  @Test
  public void onInsert() {
    SerializableInsertEvent event = mockEvent(SerializableInsertEvent.class);
    listener.onInsert(event);
    verifyZeroInteractions(metaStoreClient);
  }

}
