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
package com.hotels.shunting.yard.replicator.exec.messaging;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.shunting.yard.common.event.AddPartitionEvent;
import com.hotels.shunting.yard.common.event.AlterPartitionEvent;
import com.hotels.shunting.yard.common.event.AlterTableEvent;
import com.hotels.shunting.yard.common.event.CreateTableEvent;
import com.hotels.shunting.yard.common.event.DropPartitionEvent;
import com.hotels.shunting.yard.common.event.DropTableEvent;
import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.event.InsertTableEvent;
import com.hotels.shunting.yard.common.event.ListenerEvent;
import com.hotels.shunting.yard.common.messaging.MessageReader;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

@RunWith(MockitoJUnitRunner.class)
public class MessageReaderAdapterTest {

  private static final List<String> PARTITION_VALUES = ImmutableList.of("value_1", "value_2", "value_3");
  private static final Map<String, String> PARTITION_KEYS_MAP = ImmutableMap
      .of("column_1", "string", "column_2", "integer", "column_3", "string");
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private static final String OLD_PARTITION_LOCATION = "s3://table_location/old_partition_location";
  private static final String PARTITION_LOCATION = "s3://table_location/partition_location";
  private static final String TEST_DB = "test_db";
  private static final String TEST_TABLE = "test_table";
  private static final String OLD_TEST_TABLE_LOCATION = "s3://old_table_location";
  private static final String TEST_TABLE_LOCATION = "s3://table_location";
  private static final String SOURCE_METASTORE_URIS = "thrift://remote_host:9883";
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
  private @Mock Partition partition;

  private List<Partition> partitionValues;
  private List<FieldSchema> partitionKeys;

  @Before
  public void init() {
    FieldSchema partitionColumn1 = new FieldSchema("column_1", "string", "");
    FieldSchema partitionColumn2 = new FieldSchema("column_2", "integer", "");
    FieldSchema partitionColumn3 = new FieldSchema("column_3", "string", "");

    partitionKeys = ImmutableList.of(partitionColumn1, partitionColumn2, partitionColumn3);
    partitionValues = ImmutableList.of(partition);
    messageReaderAdapter = new MessageReaderAdapter(messageReader, SOURCE_METASTORE_URIS);
    when(partition.getValues()).thenReturn(PARTITION_VALUES);
  }

  @Test
  public void createTableEvent() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryCreateTableEvent));
    configureMockedEvent(apiaryCreateTableEvent);
    when(apiaryCreateTableEvent.getEventType()).thenReturn(EventType.CREATE_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.CREATE_TABLE, TEST_DB, TEST_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void addPartitionEvent() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryAddPartitionEvent));
    configureMockedEvent(apiaryAddPartitionEvent);

    when(apiaryAddPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(apiaryAddPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(apiaryAddPartitionEvent.getEventType()).thenReturn(EventType.ADD_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ADD_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .replicationMode(ReplicationMode.FULL)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void alterPartitionEvent() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryAlterPartitionEvent));
    configureMockedEvent(apiaryAlterPartitionEvent);

    when(apiaryAlterPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(apiaryAlterPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(apiaryAlterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PARTITION_LOCATION);
    when(apiaryAlterPartitionEvent.getPartitionLocation()).thenReturn(PARTITION_LOCATION);
    when(apiaryAlterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .replicationMode(ReplicationMode.FULL)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void alterTableEvent() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryAlterTableEvent));
    configureMockedEvent(apiaryAlterTableEvent);

    when(apiaryAlterTableEvent.getTableLocation()).thenReturn(TEST_TABLE_LOCATION);
    when(apiaryAlterTableEvent.getOldTableLocation()).thenReturn(OLD_TEST_TABLE_LOCATION);
    when(apiaryAlterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_TABLE, TEST_DB, TEST_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void metadataOnlySyncEventForPartition() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryAlterPartitionEvent));
    configureMockedEvent(apiaryAlterPartitionEvent);

    when(apiaryAlterPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(apiaryAlterPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(apiaryAlterPartitionEvent.getOldPartitionLocation()).thenReturn(PARTITION_LOCATION);
    when(apiaryAlterPartitionEvent.getPartitionLocation()).thenReturn(PARTITION_LOCATION);
    when(apiaryAlterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .replicationMode(ReplicationMode.METADATA_UPDATE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void metadataOnlySyncEventForPartitionWithNullLocations() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryAlterPartitionEvent));
    configureMockedEvent(apiaryAlterPartitionEvent);

    when(apiaryAlterPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(apiaryAlterPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(apiaryAlterPartitionEvent.getOldPartitionLocation()).thenReturn(null);
    when(apiaryAlterPartitionEvent.getPartitionLocation()).thenReturn(null);
    when(apiaryAlterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .replicationMode(ReplicationMode.FULL)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void metadataOnlySyncEventForTable() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryAlterTableEvent));
    configureMockedEvent(apiaryAlterTableEvent);

    when(apiaryAlterTableEvent.getTableLocation()).thenReturn(TEST_TABLE_LOCATION);
    when(apiaryAlterTableEvent.getOldTableLocation()).thenReturn(TEST_TABLE_LOCATION);
    when(apiaryAlterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_TABLE, TEST_DB, TEST_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.METADATA_UPDATE)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void metadataOnlySyncEventForTableWithNullLocation() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryAlterTableEvent));
    configureMockedEvent(apiaryAlterTableEvent);

    when(apiaryAlterTableEvent.getTableLocation()).thenReturn(null);
    when(apiaryAlterTableEvent.getOldTableLocation()).thenReturn(null);
    when(apiaryAlterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_TABLE, TEST_DB, TEST_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .replicationMode(ReplicationMode.FULL)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void dropPartitionEvent() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryDropPartitionEvent));
    configureMockedEvent(apiaryDropPartitionEvent);

    when(apiaryDropPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(apiaryDropPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(apiaryDropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.DROP_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .deleteData(true)
        .parameters(PARAMETERS)
        .environmentContext(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void insertTableEvent() {
    Map<String, String> partitionKeyValues = IntStream
        .range(0, partitionKeys.size())
        .collect(LinkedHashMap::new,
            (m, i) -> m.put(partitionKeys.get(i).getName(), partitionValues.get(0).getValues().get(i)), Map::putAll);

    when(messageReader.next()).thenReturn(Optional.of(apiaryInsertTableEvent));
    configureMockedEvent(apiaryInsertTableEvent);

    when(apiaryInsertTableEvent.getPartitionKeyValues()).thenReturn(partitionKeyValues);
    when(apiaryInsertTableEvent.getEventType()).thenReturn(EventType.INSERT);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.INSERT, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void dropTableEvent() {
    when(messageReader.next()).thenReturn(Optional.of(apiaryDropTableEvent));
    configureMockedEvent(apiaryDropTableEvent);
    when(apiaryDropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.DROP_TABLE, TEST_DB, TEST_TABLE)
        .deleteData(true)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next().get();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void testClose() throws IOException {
    messageReaderAdapter.close();
    verify(messageReader).close();
  }

  private void configureMockedEvent(ListenerEvent serializableListenerEvent) {
    when(serializableListenerEvent.getDbName()).thenReturn(TEST_DB);
    when(serializableListenerEvent.getTableName()).thenReturn(TEST_TABLE);
  }

  private void assertMetaStoreEvent(MetaStoreEvent expected, MetaStoreEvent actual) {
    assertThat(actual.getEventType()).isEqualTo(expected.getEventType());
    assertThat(actual.getDatabaseName()).isEqualTo(expected.getDatabaseName());
    assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
    assertThat(actual.getPartitionColumns()).isEqualTo(expected.getPartitionColumns());
    assertThat(actual.getPartitionValues()).isEqualTo(expected.getPartitionValues());
    assertThat(actual.getReplicationMode()).isEqualTo(expected.getReplicationMode());
    assertThat(actual.isDeleteData()).isEqualTo(expected.isDeleteData());
    assertThat(actual.getParameters()).isEqualTo(expected.getParameters());
    assertThat(actual.getEnvironmentContext()).isEqualTo(expected.getEnvironmentContext());
  }

}
