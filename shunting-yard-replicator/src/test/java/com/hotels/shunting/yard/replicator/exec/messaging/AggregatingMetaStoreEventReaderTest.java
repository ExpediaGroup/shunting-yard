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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.hotels.shunting.yard.common.io.SerDeException;
import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.event.aggregation.MetaStoreEventAggregator;

@RunWith(MockitoJUnitRunner.class)
public class AggregatingMetaStoreEventReaderTest {

  private static final int WINDOW = 5;
  private static final TimeUnit WINDOW_UNITS = TimeUnit.SECONDS;

  public @Rule ExpectedException expectedException = ExpectedException.none();

  private @Mock MetaStoreEventReader delegate;
  private @Mock MetaStoreEventAggregator aggregator;
  private @Mock Queue<MetaStoreEvent> buffer;
  private @Captor ArgumentCaptor<List<MetaStoreEvent>> eventsCaptor;

  private AggregatingMetaStoreEventReader aggregatingMessageReader;

  @Before
  public void init() {
    when(buffer.isEmpty()).thenReturn(true);
    when(buffer.addAll(any())).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        when(buffer.isEmpty()).thenReturn(false);
        return Boolean.TRUE;
      }
    });
    when(aggregator.aggregate(any())).thenAnswer(new Answer<List<MetaStoreEvent>>() {
      @Override
      public List<MetaStoreEvent> answer(InvocationOnMock invocation) throws Throwable {
        return (List<MetaStoreEvent>) invocation.getArgument(0);
      }
    });
    aggregatingMessageReader = new AggregatingMetaStoreEventReader(delegate, aggregator, WINDOW, WINDOW_UNITS, buffer);
  }

  @Test
  public void readExceedsTheWindow() {
    List<MetaStoreEvent> events = Arrays.asList(mock(MetaStoreEvent.class));
    when(delegate.next()).thenAnswer(new Answer<MetaStoreEvent>() {
      @Override
      public MetaStoreEvent answer(InvocationOnMock invocation) throws Throwable {
        WINDOW_UNITS.sleep(WINDOW + 1);
        return events.get(0);
      }
    });
    aggregatingMessageReader.next();
    verify(buffer).poll();
    verify(delegate).next();
    verify(aggregator).aggregate(events);
    verify(buffer).addAll(events);
  }

  @Test
  public void multipleReadsBeforeExceedingTheWindow() {
    MetaStoreEvent[] events = new MetaStoreEvent[] {
        mock(MetaStoreEvent.class),
        mock(MetaStoreEvent.class),
        mock(MetaStoreEvent.class),
        mock(MetaStoreEvent.class),
        mock(MetaStoreEvent.class) };
    final int counter[] = new int[] { 0 };
    when(delegate.next()).thenAnswer(new Answer<MetaStoreEvent>() {
      @Override
      public MetaStoreEvent answer(InvocationOnMock invocation) throws Throwable {
        WINDOW_UNITS.sleep(counter[0] < events.length - 1 ? 1 : WINDOW);
        return events[counter[0]++];
      }
    });
    aggregatingMessageReader.next();
    verify(buffer).poll();
    verify(delegate, atLeast(2)).next();
    verify(aggregator).aggregate(any());
    verify(buffer).addAll(eventsCaptor.capture());
    int numOfEventsCaptured = eventsCaptor.getValue().size();
    assertThat(eventsCaptor.getValue()).containsExactly(Arrays.copyOf(events, numOfEventsCaptured));
  }

  @Test
  public void failuresInDelegateArePropagated() {
    SerDeException e = new SerDeException("oops");
    expectedException.expect(is(e));
    when(delegate.next()).thenThrow(e);
    aggregatingMessageReader.next();
  }

}
