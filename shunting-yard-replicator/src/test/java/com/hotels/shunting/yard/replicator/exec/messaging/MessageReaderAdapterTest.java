package com.hotels.shunting.yard.replicator.exec.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

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
import com.hotels.shunting.yard.common.messaging.MessageReader;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

@RunWith(MockitoJUnitRunner.class)
public class MessageReaderAdapterTest {

  private static final List<String> PARTITION_VALUES = ImmutableList.of("value_1", "value_2", "value_3");
  private static final List<String> PARTITION_COLUMNS = ImmutableList.of("Column_1", "Column_2", "Column_3");
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private static final String TEST_DB = "test_db";
  private static final String TEST_TABLE = "test_table";

  private MessageReaderAdapter messageReaderAdapter;

  private @Mock SerializableAddPartitionEvent addPartitionEvent;
  private @Mock SerializableAlterPartitionEvent alterPartitionEvent;
  private @Mock SerializableDropPartitionEvent dropPartitionEvent;
  private @Mock SerializableDropTableEvent dropTableEvent;
  private @Mock SerializableInsertEvent insertEvent;
  private @Mock SerializableCreateTableEvent createTableEvent;
  private @Mock MessageReader messageReader;
  private @Mock Table dummyHiveTable;
  private @Mock Partition partition;

  private List<Partition> partitionValues;
  private List<FieldSchema> partitionKeys;

  @Before
  public void init() {
    FieldSchema partitionColumn1 = new FieldSchema("Column_1", "String", "");
    FieldSchema partitionColumn2 = new FieldSchema("Column_2", "String", "");
    FieldSchema partitionColumn3 = new FieldSchema("Column_3", "String", "");

    partitionKeys = ImmutableList.of(partitionColumn1, partitionColumn2, partitionColumn3);
    partitionValues = ImmutableList.of(partition);
    messageReaderAdapter = new MessageReaderAdapter(messageReader);
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
        .partitionColumns(PARTITION_COLUMNS)
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(actual, expected);

  }

  @Test
  public void alterPartitionEvent() {
    when(messageReader.next()).thenReturn(alterPartitionEvent);
    configureMockedEvent(alterPartitionEvent);
    when(alterPartitionEvent.getTable()).thenReturn(dummyHiveTable);
    when(alterPartitionEvent.getNewPartition()).thenReturn(partition);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ON_ALTER_PARTITION);

    MetaStoreEvent actual = messageReaderAdapter.next();

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_ALTER_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(PARTITION_COLUMNS)
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(EMPTY_MAP)
        .build();

    assertMetaStoreEvent(actual, expected);

  }

  @Test
  public void dropPartitionEvent() {
    when(messageReader.next()).thenReturn(dropPartitionEvent);
    configureMockedEvent(dropPartitionEvent);
    when(dropPartitionEvent.getTable()).thenReturn(dummyHiveTable);
    when(dropPartitionEvent.getPartitions()).thenReturn(partitionValues);
    when(dropPartitionEvent.getEventType()).thenReturn(EventType.ON_DROP_PARTITION);
    when(dropPartitionEvent.getDeleteData()).thenReturn(true);

    MetaStoreEvent actual = messageReaderAdapter.next();

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_DROP_PARTITION, TEST_DB, TEST_TABLE)
        .partitionColumns(PARTITION_COLUMNS)
        .partitionValues(PARTITION_VALUES)
        .deleteData(true)
        .environmentContext(EMPTY_MAP)
        .build();

    assertMetaStoreEvent(actual, expected);

  }

  @Test
  public void dropTableEvent() {
    when(messageReader.next()).thenReturn(dropTableEvent);
    configureMockedEvent(dropTableEvent);
    when(dropTableEvent.getEventType()).thenReturn(EventType.ON_DROP_TABLE);
    when(dropTableEvent.getDeleteData()).thenReturn(true);

    MetaStoreEvent actual = messageReaderAdapter.next();

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_DROP_TABLE, TEST_DB, TEST_TABLE)
        .deleteData(true)
        .environmentContext(EMPTY_MAP)
        .build();

    assertMetaStoreEvent(actual, expected);
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
        .partitionColumns(PARTITION_COLUMNS)
        .partitionValues(PARTITION_VALUES)
        .environmentContext(EMPTY_MAP)
        .parameters(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(actual, expected);

  }

  @Test
  public void ignoresCreateTableEvent() {
    when(messageReader.next()).thenReturn(createTableEvent);
    configureMockedEvent(createTableEvent);
    when(createTableEvent.getEventType()).thenReturn(EventType.ON_CREATE_TABLE);

    MetaStoreEvent expected = MetaStoreEvent
        .builder(EventType.ON_CREATE_TABLE, TEST_DB, TEST_TABLE)
        .environmentContext(EMPTY_MAP)
        .parameters(EMPTY_MAP)
        .build();

    MetaStoreEvent actual = messageReaderAdapter.next();

    assertMetaStoreEvent(actual, expected);

  }

  private void configureMockedEvent(SerializableListenerEvent serializableListenerEvent) {
    when(serializableListenerEvent.getDatabaseName()).thenReturn(TEST_DB);
    when(serializableListenerEvent.getTableName()).thenReturn(TEST_TABLE);
  }

  private void assertMetaStoreEvent(MetaStoreEvent actual, MetaStoreEvent expected) {
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
