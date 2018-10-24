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

import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.shunting.yard.common.event.EventType;

@RunWith(MockitoJUnitRunner.class)
public class SerializableApiaryInsertEventTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final String KEY = "key";
  private static final String VALUE = "val";
  private static final String FILE = "file";
  private static final String CHECKSUM = "checksum";

  private @Mock InsertEvent insertEvent;

  private SerializableApiaryInsertTableEvent event;

  @Before
  public void init() {
    when(insertEvent.getDb()).thenReturn(DATABASE);
    when(insertEvent.getTable()).thenReturn(TABLE);
    when(insertEvent.getPartitionKeyValues()).thenReturn(ImmutableMap.of(KEY, VALUE));
    when(insertEvent.getFiles()).thenReturn(ImmutableList.of(FILE));
    when(insertEvent.getFileChecksums()).thenReturn(ImmutableList.of(CHECKSUM));
    event = new SerializableApiaryInsertTableEvent(insertEvent);
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
    assertThat(event.getEventType()).isSameAs(EventType.INSERT);
  }

  @Test
  public void keyValues() {
    assertThat(event.getPartitionKeyValues()).isEqualTo(ImmutableMap.of(KEY, VALUE));
  }

  @Test
  public void files() {
    assertThat(event.getFiles()).isEqualTo(ImmutableList.of(FILE));
  }

  @Test
  public void checksums() {
    assertThat(event.getFileChecksums()).isEqualTo(ImmutableList.of(CHECKSUM));
  }

}
