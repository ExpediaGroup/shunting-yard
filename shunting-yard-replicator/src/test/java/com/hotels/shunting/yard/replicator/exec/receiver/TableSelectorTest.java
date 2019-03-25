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
package com.hotels.shunting.yard.replicator.exec.receiver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.hotels.shunting.yard.replicator.exec.conf.SourceTableFilter;

@RunWith(MockitoJUnitRunner.class)
public class TableSelectorTest {

  private static final String DB_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";

  private TableSelector tableSelector;
  private @Mock ListenerEvent listenerEvent;

  @Before
  public void init() {
    when(listenerEvent.getDbName()).thenReturn(DB_NAME);
    when(listenerEvent.getTableName()).thenReturn(TABLE_NAME);

    SourceTableFilter targetReplication = new SourceTableFilter();
    targetReplication.setTableNames(Arrays.asList(DB_NAME + "." + TABLE_NAME, "db1.table1"));

    tableSelector = new TableSelector(targetReplication);
  }

  @Test
  public void returnsTrueWhenTableSelected() {
    assertThat(tableSelector.canProcess(listenerEvent)).isTrue();
  }

  @Test
  public void returnsFalseEventWhenTableNotSelected() {
    when(listenerEvent.getTableName()).thenReturn(TABLE_NAME + "1");
    assertThat(tableSelector.canProcess(listenerEvent)).isFalse();
  }

}
