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

import static java.util.Arrays.asList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import static com.hotels.shunting.yard.common.event.EventType.ADD_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.CREATE_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.DROP_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.DROP_TABLE;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaStoreEventCompactorTest {

  private @Mock EventMerger merger;

  private DefaultMetaStoreEventCompactor compactor;

  @Before
  public void init() {
    compactor = new DefaultMetaStoreEventCompactor(merger);
  }

  @Test
  public void shouldReturnSameEventsIfEventsCannotBeMerged() {
    List<MetaStoreEvent> events = asList(mockEvent(EventType.DROP_PARTITION), mockEvent(DROP_TABLE),
        mockEvent(CREATE_TABLE));
    List<MetaStoreEvent> compactEvents = compactor.compact(events);
    assertThat(compactEvents).isEqualTo(events);
  }

  @Test
  public void mergeEventsIfEventsCanBeMerged() {
    MetaStoreEvent eventA = mockEvent(DROP_PARTITION);
    MetaStoreEvent eventB = mockEvent(DROP_TABLE);
    MetaStoreEvent eventC = mockEvent(CREATE_TABLE);
    MetaStoreEvent eventD = mockEvent(ADD_PARTITION);
    MetaStoreEvent eventE = mockEvent(ADD_PARTITION);

    when(merger.canMerge(eventC, eventD)).thenReturn(true);
    MetaStoreEvent eventCD = mockEvent(CREATE_TABLE);
    when(merger.merge(eventC, eventD)).thenReturn(eventCD);
    when(merger.canMerge(eventCD, eventE)).thenReturn(true);
    MetaStoreEvent eventCDE = mockEvent(CREATE_TABLE);
    when(merger.merge(eventCD, eventE)).thenReturn(eventCDE);

    List<MetaStoreEvent> compactEvents = compactor.compact(asList(eventA, eventB, eventC, eventD, eventE));

    assertThat(compactEvents).isEqualTo(asList(eventA, eventB, eventCDE));
  }

  @Test
  public void mergeDiscardPreviousEventsIfADropTableIsFoundInBetween() {
    MetaStoreEvent eventA = mockEvent(CREATE_TABLE);
    MetaStoreEvent eventB = mockEvent(ADD_PARTITION);
    MetaStoreEvent eventC = mockEvent(DROP_TABLE);
    MetaStoreEvent eventD = mockEvent(CREATE_TABLE);
    MetaStoreEvent eventE = mockEvent(ADD_PARTITION);

    when(merger.canMerge(eventA, eventB)).thenReturn(true);
    MetaStoreEvent eventAB = mockEvent(CREATE_TABLE);
    when(merger.merge(eventA, eventB)).thenReturn(eventAB);

    when(merger.canMerge(eventD, eventE)).thenReturn(true);
    MetaStoreEvent eventDE = mockEvent(CREATE_TABLE);
    when(merger.merge(eventD, eventE)).thenReturn(eventDE);

    List<MetaStoreEvent> compactEvents = compactor.compact(asList(eventA, eventB, eventC, eventD, eventE));

    assertThat(compactEvents).isEqualTo(asList(eventC, eventDE));
  }

  private static MetaStoreEvent mockEvent(EventType eventType) {
    MetaStoreEvent event = mock(MetaStoreEvent.class);
    when(event.getEventType()).thenReturn(eventType);
    if (eventType == DROP_PARTITION || eventType == DROP_TABLE) {
      when(event.isDropEvent()).thenReturn(true);
    }
    return event;
  }

}
