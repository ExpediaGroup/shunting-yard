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
package com.hotels.shunting.yard.replicator.exec.event.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaStoreEventAggregatorTest {

  private static final String DATABASE = "database";
  private static final String TABLE1 = "table1";
  private static final String TABLE2 = "table2";

  private @Mock DefaultMetaStoreEventCompactor compactor;

  private DefaultMetaStoreEventAggregator aggregator;

  @Before
  public void init() {
    aggregator = new DefaultMetaStoreEventAggregator(compactor);
  }

  @Test
  public void eventsFromSingleTable() {
    List<MetaStoreEvent> events = Arrays.asList(mockEvent(DATABASE, TABLE1), mockEvent(DATABASE, TABLE1));
    when(compactor.compact(events)).thenReturn(events);
    List<MetaStoreEvent> aggregatedEvents = aggregator.aggregate(events);
    assertThat(aggregatedEvents).isEqualTo(events);
    verify(compactor).compact(events);
  }

  @Test
  public void eventsFromMultipleTables() {
    List<MetaStoreEvent> table1Events = Arrays.asList(mockEvent(DATABASE, TABLE1));
    List<MetaStoreEvent> table2Events = Arrays.asList(mockEvent(DATABASE, TABLE2));
    List<MetaStoreEvent> events = new ArrayList<>();
    events.addAll(table1Events);
    events.addAll(table2Events);
    when(compactor.compact(table1Events)).thenReturn(table1Events);
    when(compactor.compact(table2Events)).thenReturn(table2Events);
    List<MetaStoreEvent> aggregatedEvents = aggregator.aggregate(events);
    assertThat(aggregatedEvents).isEqualTo(events);
    verify(compactor).compact(table1Events);
    verify(compactor).compact(table2Events);
  }

  private static MetaStoreEvent mockEvent(String database, String table) {
    MetaStoreEvent event = mock(MetaStoreEvent.class);
    when(event.getQualifiedTableName()).thenReturn(database + "." + table);
    return event;
  }
}
