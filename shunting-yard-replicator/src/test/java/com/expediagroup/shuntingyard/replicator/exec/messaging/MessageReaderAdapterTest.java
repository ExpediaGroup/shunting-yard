/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.expediagroup.shuntingyard.replicator.exec.messaging;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.InsertTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageProperty;

import com.expediagroup.shuntingyard.replicator.exec.conf.ShuntingYardTableReplicationsMap;
import com.expediagroup.shuntingyard.replicator.exec.conf.ct.ShuntingYardTableReplication;
import com.expediagroup.shuntingyard.replicator.exec.conf.ct.ShuntingYardTableReplications;
import com.expediagroup.shuntingyard.replicator.exec.event.MetaStoreEvent;

import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class MessageReaderAdapterTest {

  private static final List<String> PARTITION_VALUES = ImmutableList.of("value_1", "value_2", "value_3");
  private static final Map<String, String> PARTITION_KEYS_MAP = ImmutableMap
      .of("column_1", "string", "column_2", "integer", "column_3", "string");
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private static final String TEST_DB = "test_db";
  private static final String TEST_TABLE = "test_table";
  private static final String OLD_TEST_TABLE_LOCATION = "s3://old_table_location";
  private static final String TEST_TABLE_LOCATION = "s3://table_location";
  private static final String SOURCE_METASTORE_URIS = "thrift://remote_host:9883";
  private static final String RECEIPT_HANDLE = "receiptHandle";
  private static final String REPLICA_DATABASE = "replica_db";
  private static final String REPLICA_TABLE = "replica_table";
  private static final Map<String, String> PARAMETERS = ImmutableMap.of(METASTOREURIS.varname, SOURCE_METASTORE_URIS);

  private MessageReaderAdapter messageReaderAdapter;

  private @Mock AddPartitionEvent apiaryAddPartitionEvent;
  private @Mock AlterPartitionEvent apiaryAlterPartitionEvent;
  private @Mock AlterTableEvent apiaryAlterTableEvent;
  private @Mock DropPartitionEvent apiaryDropPartitionEvent;
  private @Mock CreateTableEvent apiaryCreateTableEvent;
  private @Mock InsertTableEvent apiaryInsertTableEvent;
  private @Mock DropTableEvent apiaryDropTableEvent;

  private @Mock MessageReader messageReader;
  private @Mock CloseableMetaStoreClient metaStoreClient;
  private @Mock Table hiveTable;
  private @Mock Partition partition;

  private List<Partition> partitionValues;
  private List<FieldSchema> partitionKeys;

  @Before
  public void init() throws MetaException, NoSuchObjectException, TException {
    FieldSchema partitionColumn1 = new FieldSchema("column_1", "string", "");
    FieldSchema partitionColumn2 = new FieldSchema("column_2", "integer", "");
    FieldSchema partitionColumn3 = new FieldSchema("column_3", "string", "");

    ShuntingYardTableReplication tableReplication = new ShuntingYardTableReplication();
    SourceTable sourceTable = new SourceTable();
    sourceTable.setDatabaseName(TEST_DB);
    sourceTable.setTableName(TEST_TABLE);

    ReplicaTable replicaTable = new ReplicaTable();
    replicaTable.setDatabaseName(REPLICA_DATABASE);
    replicaTable.setTableName(REPLICA_TABLE);

    tableReplication.setSourceTable(sourceTable);
    tableReplication.setReplicaTable(replicaTable);

    List<ShuntingYardTableReplication> tableReplications = new ArrayList<>();
    tableReplications.add(tableReplication);

    ShuntingYardTableReplications tableReplicationsWrapper = new ShuntingYardTableReplications();
    tableReplicationsWrapper.setTableReplications(tableReplications);

    partitionKeys = ImmutableList.of(partitionColumn1, partitionColumn2, partitionColumn3);
    partitionValues = ImmutableList.of(partition);

    when(hiveTable.getPartitionKeys()).thenReturn(partitionKeys);
    when(metaStoreClient.getTable(TEST_DB, TEST_TABLE)).thenReturn(hiveTable);

    messageReaderAdapter = new MessageReaderAdapter(messageReader, SOURCE_METASTORE_URIS, metaStoreClient,
        new ShuntingYardTableReplicationsMap(tableReplicationsWrapper));
    when(partition.getValues()).thenReturn(PARTITION_VALUES);
  }

  @Test
  public void createTableEvent() {
    when(messageReader.read()).thenReturn(newMessageEvent(apiaryCreateTableEvent));
    configureMockedEvent(apiaryCreateTableEvent);
    when(apiaryCreateTableEvent.getEventType()).thenReturn(EventType.CREATE_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.CREATE_TABLE, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void createTableEventWhenTableReplicationsAreNotConfigured() {
    messageReaderAdapter = new MessageReaderAdapter(messageReader, SOURCE_METASTORE_URIS, metaStoreClient,
        new ShuntingYardTableReplicationsMap(null));

    when(messageReader.read()).thenReturn(newMessageEvent(apiaryCreateTableEvent));
    configureMockedEvent(apiaryCreateTableEvent);
    when(apiaryCreateTableEvent.getEventType()).thenReturn(EventType.CREATE_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.CREATE_TABLE, TEST_DB, TEST_TABLE, TEST_DB, TEST_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void createTableEventWhenReplicaTableNameNotProvided() {
    ShuntingYardTableReplication tableReplication = new ShuntingYardTableReplication();
    SourceTable sourceTable = new SourceTable();
    sourceTable.setDatabaseName(TEST_DB);
    sourceTable.setTableName(TEST_TABLE);

    ReplicaTable replicaTable = new ReplicaTable();
    replicaTable.setDatabaseName(REPLICA_DATABASE);

    tableReplication.setSourceTable(sourceTable);
    tableReplication.setReplicaTable(replicaTable);

    List<ShuntingYardTableReplication> tableReplications = new ArrayList<>();
    tableReplications.add(tableReplication);

    ShuntingYardTableReplications tableReplicationsWrapper = new ShuntingYardTableReplications();
    tableReplicationsWrapper.setTableReplications(tableReplications);

    messageReaderAdapter = new MessageReaderAdapter(messageReader, SOURCE_METASTORE_URIS, metaStoreClient,
        new ShuntingYardTableReplicationsMap(tableReplicationsWrapper));

    Optional<MessageEvent> messageEvent = newMessageEvent(apiaryCreateTableEvent);
    when(messageReader.read()).thenReturn(messageEvent);
    configureMockedEvent(apiaryCreateTableEvent);
    when(apiaryCreateTableEvent.getEventType()).thenReturn(EventType.CREATE_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.CREATE_TABLE, TEST_DB, TEST_TABLE, REPLICA_DATABASE, TEST_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(messageEvent.get());
  }

  @Test
  public void addPartitionEvent() {
    Optional<MessageEvent> event = newMessageEvent(apiaryAddPartitionEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryAddPartitionEvent);

    when(apiaryAddPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(apiaryAddPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(apiaryAddPartitionEvent.getEventType()).thenReturn(EventType.ADD_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ADD_PARTITION, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .replicationMode(ReplicationMode.FULL)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void alterPartitionEvent() {
    Optional<MessageEvent> event = newMessageEvent(apiaryAlterPartitionEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryAlterPartitionEvent);

    when(apiaryAlterPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(apiaryAlterPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(apiaryAlterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_PARTITION, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .replicationMode(ReplicationMode.FULL)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void alterTableEvent() {
    Optional<MessageEvent> event = newMessageEvent(apiaryAlterTableEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryAlterTableEvent);

    when(apiaryAlterTableEvent.getTableLocation()).thenReturn(TEST_TABLE_LOCATION);
    when(apiaryAlterTableEvent.getOldTableLocation()).thenReturn(OLD_TEST_TABLE_LOCATION);
    when(apiaryAlterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_TABLE, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void metadataOnlySyncEventForPartitionWithNullLocations() {
    Optional<MessageEvent> event = newMessageEvent(apiaryAlterPartitionEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryAlterPartitionEvent);

    when(apiaryAlterPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(apiaryAlterPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(apiaryAlterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_PARTITION, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .replicationMode(ReplicationMode.FULL)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void metadataOnlySyncEventForTable() {
    Optional<MessageEvent> event = newMessageEvent(apiaryAlterTableEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryAlterTableEvent);

    when(apiaryAlterTableEvent.getTableLocation()).thenReturn(TEST_TABLE_LOCATION);
    when(apiaryAlterTableEvent.getOldTableLocation()).thenReturn(TEST_TABLE_LOCATION);
    when(apiaryAlterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_TABLE, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.METADATA_UPDATE)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void metadataOnlySyncEventForTableWithNullLocation() {
    Optional<MessageEvent> event = newMessageEvent(apiaryAlterTableEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryAlterTableEvent);

    when(apiaryAlterTableEvent.getTableLocation()).thenReturn(null);
    when(apiaryAlterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_TABLE, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void insertOverwriteEventForUnpartitionedTable() throws TException {
    Optional<MessageEvent> event = newMessageEvent(apiaryAlterTableEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryAlterTableEvent);

    when(metaStoreClient.getTable(TEST_DB, TEST_TABLE)).thenReturn(hiveTable);
    when(hiveTable.getPartitionKeys()).thenReturn(Lists.newArrayList());
    when(apiaryAlterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_TABLE, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void insertOverwriteEventForUnpartitionedTableWhenPartitionKeysAreNull() throws TException {
    Optional<MessageEvent> event = newMessageEvent(apiaryAlterTableEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryAlterTableEvent);

    when(metaStoreClient.getTable(TEST_DB, TEST_TABLE)).thenReturn(hiveTable);
    when(hiveTable.getPartitionKeys()).thenReturn(null);
    when(apiaryAlterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_TABLE, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void dropPartitionEvent() {
    Optional<MessageEvent> event = newMessageEvent(apiaryDropPartitionEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryDropPartitionEvent);

    when(apiaryDropPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(apiaryDropPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(apiaryDropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.DROP_PARTITION, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .deleteData(true)
        .parameters(PARAMETERS)
        .environmentContext(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void insertTableEvent() {
    Map<String, String> partitionKeyValues = IntStream
        .range(0, partitionKeys.size())
        .collect(LinkedHashMap::new,
            (m, i) -> m.put(partitionKeys.get(i).getName(), partitionValues.get(0).getValues().get(i)), Map::putAll);

    Optional<MessageEvent> event = newMessageEvent(apiaryInsertTableEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryInsertTableEvent);

    when(apiaryInsertTableEvent.getPartitionKeyValues()).thenReturn(partitionKeyValues);
    when(apiaryInsertTableEvent.getEventType()).thenReturn(EventType.INSERT);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.INSERT, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void dropTableEvent() {
    Optional<MessageEvent> event = newMessageEvent(apiaryDropTableEvent);
    when(messageReader.read()).thenReturn(event);
    configureMockedEvent(apiaryDropTableEvent);
    when(apiaryDropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.DROP_TABLE, TEST_DB, TEST_TABLE, REPLICA_DATABASE, REPLICA_TABLE)
        .deleteData(true)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.read().get();

    assertMetaStoreEvent(expected, actual);
    verify(messageReader).delete(event.get());
  }

  @Test
  public void testClose() throws IOException {
    messageReaderAdapter.close();
    verify(messageReader).close();
  }

  private Optional<MessageEvent> newMessageEvent(ListenerEvent event) {
    return Optional
        .of(new MessageEvent(event,
            Collections.singletonMap(SqsMessageProperty.SQS_MESSAGE_RECEIPT_HANDLE, RECEIPT_HANDLE)));
  }

  private void configureMockedEvent(ListenerEvent serializableListenerEvent) {
    when(serializableListenerEvent.getDbName()).thenReturn(TEST_DB);
    when(serializableListenerEvent.getTableName()).thenReturn(TEST_TABLE);
  }

  private void assertMetaStoreEvent(MetaStoreEvent expected, MetaStoreEvent actual) {
    assertThat(actual.getEventType()).isEqualTo(expected.getEventType());
    assertThat(actual.getDatabaseName()).isEqualTo(expected.getDatabaseName());
    assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
    assertThat(actual.getReplicaDatabaseName()).isEqualTo(expected.getReplicaDatabaseName());
    assertThat(actual.getReplicaTableName()).isEqualTo(expected.getReplicaTableName());
    assertThat(actual.getPartitionColumns()).isEqualTo(expected.getPartitionColumns());
    assertThat(actual.getPartitionValues()).isEqualTo(expected.getPartitionValues());
    assertThat(actual.getReplicationMode()).isEqualTo(expected.getReplicationMode());
    assertThat(actual.isDeleteData()).isEqualTo(expected.isDeleteData());
    assertThat(actual.getParameters()).isEqualTo(expected.getParameters());
    assertThat(actual.getEnvironmentContext()).isEqualTo(expected.getEnvironmentContext());
  }

}
