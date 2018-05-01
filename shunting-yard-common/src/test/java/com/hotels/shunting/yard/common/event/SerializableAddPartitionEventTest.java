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
package com.hotels.shunting.yard.common.event;

import static java.util.Collections.EMPTY_LIST;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerializableAddPartitionEventTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";

  private @Mock AddPartitionEvent addPartitionEvent;
  private @Mock Table table;
  private @Mock Partition partition;

  private SerializableAddPartitionEvent event;

  @Before
  public void init() {
    when(table.getDbName()).thenReturn(DATABASE);
    when(table.getTableName()).thenReturn(TABLE);
    when(addPartitionEvent.getTable()).thenReturn(table);
    when(addPartitionEvent.getPartitionIterator()).thenReturn(Arrays.asList(partition).iterator());
    when(addPartitionEvent.getStatus()).thenReturn(true);
    event = new SerializableAddPartitionEvent(addPartitionEvent);
  }

  @Test
  public void databaseName() {
    assertThat(event.getDatabaseName()).isEqualTo(DATABASE);
  }

  @Test
  public void tableName() {
    assertThat(event.getTableName()).isEqualTo(TABLE);
  }

  @Test
  public void eventType() {
    assertThat(event.getEventType()).isSameAs(EventType.ON_ADD_PARTITION);
  }

  @Test
  public void table() {
    assertThat(event.getTable()).isSameAs(table);
  }

  @Test
  public void partitions() {
    assertThat(event.getPartitions()).isEqualTo(Arrays.asList(partition));
    assertThat(event.getPartitions()).isEqualTo(Arrays.asList(partition));
  }

  @Test(expected = NullPointerException.class)
  public void nullPartitionIterator() {
    when(addPartitionEvent.getPartitionIterator()).thenReturn(null);
    new SerializableAddPartitionEvent(addPartitionEvent);
  }

  @Test
  public void emptyPartitionIterator() {
    when(addPartitionEvent.getPartitionIterator()).thenReturn(EMPTY_LIST.iterator());
    SerializableAddPartitionEvent event = new SerializableAddPartitionEvent(addPartitionEvent);
    assertThat(event.getPartitions()).isEqualTo(EMPTY_LIST);
  }

}
