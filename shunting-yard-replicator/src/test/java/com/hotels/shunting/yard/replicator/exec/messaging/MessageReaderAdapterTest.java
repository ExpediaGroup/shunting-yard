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
import java.util.stream.IntStream;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.event.SerializableAddPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableCreateTableEvent;
import com.hotels.shunting.yard.common.event.SerializableDropPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropTableEvent;
import com.hotels.shunting.yard.common.event.SerializableInsertEvent;
import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAddPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryCreateTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryInsertTableEvent;
import com.hotels.shunting.yard.common.messaging.MessageReader;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

@RunWith(MockitoJUnitRunner.class)
public class MessageReaderAdapterTest {

  private static final List<String> PARTITION_VALUES = ImmutableList.of("value_1", "value_2", "value_3");
  private static final Map<String, String> PARTITION_KEYS_MAP = ImmutableMap
      .of("column_1", "string", "column_2", "integer", "column_3", "string");
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private static final String TEST_DB = "test_db";
  private static final String TEST_TABLE = "test_table";
  private static final String SOURCE_METASTORE_URIS = "thrift://remote_host:9883";
  private static final Map<String, String> PARAMETERS = ImmutableMap.of(METASTOREURIS.varname, SOURCE_METASTORE_URIS);

  private MessageReaderAdapter messageReaderAdapter;

  private @Mock SerializableAddPartitionEvent addPartitionEvent;
  private @Mock SerializableAlterPartitionEvent alterPartitionEvent;
  private @Mock SerializableDropPartitionEvent dropPartitionEvent;
  private @Mock SerializableDropTableEvent dropTableEvent;
  private @Mock SerializableInsertEvent insertEvent;
  private @Mock SerializableCreateTableEvent createTableEvent;

  private @Mock SerializableApiaryAddPartitionEvent addApiaryPartitionEvent;
  private @Mock SerializableApiaryAlterPartitionEvent alterApiaryPartitionEvent;
  private @Mock SerializableApiaryDropPartitionEvent dropApiaryPartitionEvent;
  private @Mock SerializableApiaryCreateTableEvent createApiaryTableEvent;
  private @Mock SerializableApiaryInsertTableEvent insertApiaryTableEvent;
  private @Mock SerializableApiaryDropTableEvent dropApiaryTableEvent;

  private @Mock MessageReader messageReader;
  private @Mock Table dummyHiveTable;
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
    when(dummyHiveTable.getPartitionKeys()).thenReturn(partitionKeys);
  }

  @Test
  public void addPartitionEvent() {
    when(messageReader.next()).thenReturn(addPartitionEvent);
    configureMockedEvent(addPartitionEvent);
    when(addPartitionEvent.getTable()).thenReturn(dummyHiveTable);
    when(addPartitionEvent.getPartitions()).thenReturn(partitionValues);
    when(addPartitionEvent.getEventType()).thenReturn(EventType.ON_ADD_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_ADD_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void alterPartitionEvent() {
    when(messageReader.next()).thenReturn(alterPartitionEvent);
    configureMockedEvent(alterPartitionEvent);
    when(alterPartitionEvent.getTable()).thenReturn(dummyHiveTable);
    when(alterPartitionEvent.getNewPartition()).thenReturn(partition);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ON_ALTER_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_ALTER_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void dropPartitionEvent() {
    when(messageReader.next()).thenReturn(dropPartitionEvent);
    configureMockedEvent(dropPartitionEvent);
    when(dropPartitionEvent.getTable()).thenReturn(dummyHiveTable);
    when(dropPartitionEvent.getPartitions()).thenReturn(partitionValues);
    when(dropPartitionEvent.getEventType()).thenReturn(EventType.ON_DROP_PARTITION);
    when(dropPartitionEvent.getDeleteData()).thenReturn(true);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_DROP_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .deleteData(true)
        .environmentContext(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void dropTableEvent() {
    when(messageReader.next()).thenReturn(dropTableEvent);
    configureMockedEvent(dropTableEvent);
    when(dropTableEvent.getEventType()).thenReturn(EventType.ON_DROP_TABLE);
    when(dropTableEvent.getDeleteData()).thenReturn(true);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_DROP_TABLE, TEST_DB, TEST_TABLE)
        .deleteData(true)
        .environmentContext(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void insertEvent() {
    Map<String, String> partitionKeyValues = IntStream
        .range(0, partitionKeys.size())
        .collect(LinkedHashMap::new,
            (m, i) -> m.put(partitionKeys.get(i).getName(), partitionValues.get(0).getValues().get(i)), Map::putAll);

    when(messageReader.next()).thenReturn(insertEvent);
    configureMockedEvent(insertEvent);
    when(insertEvent.getKeyValues()).thenReturn(partitionKeyValues);
    when(insertEvent.getEventType()).thenReturn(EventType.ON_INSERT);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_INSERT, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void ignoresCreateTableEvent() {
    when(messageReader.next()).thenReturn(createApiaryTableEvent);
    configureMockedEvent(createApiaryTableEvent);
    when(createApiaryTableEvent.getEventType()).thenReturn(EventType.ON_CREATE_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_CREATE_TABLE, TEST_DB, TEST_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void apiaryAddPartitionEvent() {
    when(messageReader.next()).thenReturn(addApiaryPartitionEvent);
    configureMockedEvent(addApiaryPartitionEvent);

    when(addApiaryPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(addApiaryPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(addApiaryPartitionEvent.getEventType()).thenReturn(EventType.ADD_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ADD_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void apiaryAlterPartitionEvent() {
    when(messageReader.next()).thenReturn(alterApiaryPartitionEvent);
    configureMockedEvent(alterApiaryPartitionEvent);

    when(alterApiaryPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(alterApiaryPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(alterApiaryPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ALTER_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void apiaryDropPartitionEvent() {
    when(messageReader.next()).thenReturn(dropApiaryPartitionEvent);
    configureMockedEvent(dropApiaryPartitionEvent);

    when(dropApiaryPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS_MAP);
    when(dropApiaryPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);
    when(dropApiaryPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.DROP_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .deleteData(true)
        .parameters(PARAMETERS)
        .environmentContext(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void apiaryInsertTableEvent() {
    Map<String, String> partitionKeyValues = IntStream
        .range(0, partitionKeys.size())
        .collect(LinkedHashMap::new,
            (m, i) -> m.put(partitionKeys.get(i).getName(), partitionValues.get(0).getValues().get(i)), Map::putAll);

    when(messageReader.next()).thenReturn(insertApiaryTableEvent);
    configureMockedEvent(insertApiaryTableEvent);

    when(insertApiaryTableEvent.getPartitionKeyValues()).thenReturn(partitionKeyValues);
    when(insertApiaryTableEvent.getEventType()).thenReturn(EventType.INSERT);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.INSERT, TEST_DB, TEST_TABLE)
        .partitionColumns(new ArrayList<String>(PARTITION_KEYS_MAP.keySet()))
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void apiaryDropTableEvent() {
    when(messageReader.next()).thenReturn(dropApiaryTableEvent);
    configureMockedEvent(dropApiaryTableEvent);
    when(dropApiaryTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.DROP_TABLE, TEST_DB, TEST_TABLE)
        .deleteData(true)
        .environmentContext(EMPTY_MAP)
        .parameters(PARAMETERS)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(expected, actual);
  }

  @Test
  public void testHasNext() {
    messageReaderAdapter.hasNext();
    verify(messageReader).hasNext();
  }

  @Test
  public void testClose() throws IOException {
    messageReaderAdapter.close();
    verify(messageReader).close();
  }

  private void configureMockedEvent(SerializableListenerEvent serializableListenerEvent) {
    when(serializableListenerEvent.getDatabaseName()).thenReturn(TEST_DB);
    when(serializableListenerEvent.getTableName()).thenReturn(TEST_TABLE);
  }

  private void assertMetaStoreEvent(MetaStoreEvent expected, MetaStoreEvent actual) {
    assertThat(actual.getEventType()).isEqualTo(expected.getEventType());
    assertThat(actual.getDatabaseName()).isEqualTo(expected.getDatabaseName());
    assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
    assertThat(actual.getPartitionColumns()).isEqualTo(expected.getPartitionColumns());
    assertThat(actual.getPartitionValues()).isEqualTo(expected.getPartitionValues());
    assertThat(actual.isDeleteData()).isEqualTo(expected.isDeleteData());
    assertThat(actual.getParameters()).isEqualTo(expected.getParameters());
    assertThat(actual.getEnvironmentContext()).isEqualTo(expected.getEnvironmentContext());
  }

}
