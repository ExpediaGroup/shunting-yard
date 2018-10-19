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
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerializableAlterTableEventTest {

  private static final String NEW_DATABASE = "new_db";
  private static final String NEW_TABLE = "new_tbl";

  private @Mock AlterTableEvent alterTableEvent;
  private @Mock Table newTable;
  private @Mock Table oldTable;

  private SerializableAlterTableEvent event;

  @Before
  public void init() {
    when(newTable.getDbName()).thenReturn(NEW_DATABASE);
    when(newTable.getTableName()).thenReturn(NEW_TABLE);
    when(alterTableEvent.getNewTable()).thenReturn(newTable);
    when(alterTableEvent.getOldTable()).thenReturn(oldTable);
    event = new SerializableAlterTableEvent(alterTableEvent);
  }

  @Test
  public void databaseName() {
    assertThat(event.getDatabaseName()).isEqualTo(NEW_DATABASE);
  }

  @Test
  public void tableName() {
    assertThat(event.getTableName()).isEqualTo(NEW_TABLE);
  }

  @Test
  public void eventType() {
    assertThat(event.getEventType()).isSameAs(EventType.ON_ALTER_TABLE);
  }

  @Test
  public void tables() {
    assertThat(event.getNewTable()).isSameAs(newTable);
    assertThat(event.getOldTable()).isSameAs(oldTable);
  }

}
