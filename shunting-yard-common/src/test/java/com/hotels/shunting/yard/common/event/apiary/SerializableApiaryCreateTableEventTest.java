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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.shunting.yard.common.event.EventType;

@RunWith(MockitoJUnitRunner.class)
public class SerializableApiaryCreateTableEventTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";

  private @Mock CreateTableEvent createTableEvent;
  private @Mock Table table;

  private SerializableApiaryCreateTableEvent event;

  @Before
  public void init() {
    when(table.getDbName()).thenReturn(DATABASE);
    when(table.getTableName()).thenReturn(TABLE);
    when(createTableEvent.getTable()).thenReturn(table);
    event = new SerializableApiaryCreateTableEvent(createTableEvent);
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
    assertThat(event.getEventType()).isSameAs(EventType.CREATE_TABLE);
  }
}
