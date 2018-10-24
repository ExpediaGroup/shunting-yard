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
package com.hotels.shunting.yard.common.event.apiary;

import static java.util.Collections.EMPTY_LIST;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.hotels.shunting.yard.common.event.EventType;

@RunWith(MockitoJUnitRunner.class)
public class SerializableApiaryAddPartitionEventTest {
  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final List<String> PARTITION_VALUES = ImmutableList.of("value_1", "value_2", "value_3");

  private @Mock AddPartitionEvent addPartitionEvent;
  private @Mock Table table;
  private @Mock Partition partition;
  private @Mock StorageDescriptor sd;

  private SerializableApiaryAddPartitionEvent event;

  private List<FieldSchema> partitionKeys;

  @Before
  public void init() {
    FieldSchema partitionColumn1 = new FieldSchema("column_1", "string", "");
    FieldSchema partitionColumn2 = new FieldSchema("column_2", "string", "");
    FieldSchema partitionColumn3 = new FieldSchema("column_3", "string", "");

    partitionKeys = ImmutableList.of(partitionColumn1, partitionColumn2, partitionColumn3);

    when(table.getDbName()).thenReturn(DATABASE);
    when(table.getTableName()).thenReturn(TABLE);
    when(partition.getValues()).thenReturn(PARTITION_VALUES);
    when(partition.getSd()).thenReturn(sd);
    when(sd.getCols()).thenReturn(partitionKeys);
    when(addPartitionEvent.getTable()).thenReturn(table);
    when(addPartitionEvent.getPartitionIterator()).thenReturn(Arrays.asList(partition).iterator());
    when(addPartitionEvent.getStatus()).thenReturn(true);
    event = new SerializableApiaryAddPartitionEvent(addPartitionEvent);
  }

  @Test
  public void databaseName() {
    assertThat(event.getDbName()).isEqualTo(DATABASE);
  }

  @Test
  public void tableName() {
    assertThat(event.getTableName()).isEqualTo(TABLE);
  }

  @Test
  public void eventType() {
    assertThat(event.getEventType()).isSameAs(EventType.ADD_PARTITION);
  }

  @Test
  public void partitions() {
    LinkedHashMap<String, String> partitionKeysMap = new LinkedHashMap<>();

    for (int i = 0; i < partitionKeys.size(); i++) {
      partitionKeysMap.put(partitionKeys.get(i).getName(), partitionKeys.get(i).getType());
    }

    assertThat(event.getPartitionValues()).isEqualTo(PARTITION_VALUES);
    assertThat(event.getPartitionKeys()).isEqualTo(partitionKeysMap);
  }

  @Test(expected = NullPointerException.class)
  public void nullPartitionIterator() {
    when(addPartitionEvent.getPartitionIterator()).thenReturn(null);
    new SerializableApiaryAddPartitionEvent(addPartitionEvent);
  }

  @Test
  public void emptyPartitionIterator() {
    when(addPartitionEvent.getPartitionIterator()).thenReturn(EMPTY_LIST.iterator());
    SerializableApiaryAddPartitionEvent event = new SerializableApiaryAddPartitionEvent(addPartitionEvent);
    assertThat(event.getPartitionValues()).isEqualTo(EMPTY_LIST);
  }

}
