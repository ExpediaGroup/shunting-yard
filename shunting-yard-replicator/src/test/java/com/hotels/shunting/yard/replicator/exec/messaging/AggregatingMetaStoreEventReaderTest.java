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
package com.hotels.shunting.yard.replicator.exec.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
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

import com.expedia.apiary.extensions.receiver.common.error.SerDeException;

import com.hotels.shunting.yard.replicator.exec.event.MetaStoreEvent;
import com.hotels.shunting.yard.replicator.exec.event.aggregation.MetaStoreEventAggregator;

@RunWith(MockitoJUnitRunner.class)
public class AggregatingMetaStoreEventReaderTest {

  private static final int WINDOW = 5;
  private static final TimeUnit WINDOW_UNITS = TimeUnit.SECONDS;

  public @Rule ExpectedException expectedException = ExpectedException.none();

  private @Mock MetaStoreEventReader delegate;
  private @Mock MetaStoreEventAggregator aggregator;
  private Queue<MetaStoreEvent> buffer = new LinkedList<>();
  private @Captor ArgumentCaptor<List<MetaStoreEvent>> eventsCaptor;

  private AggregatingMetaStoreEventReader aggregatingMessageReader;

  @Before
  public void init() {
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
    when(delegate.read()).thenAnswer(new Answer<Optional<MetaStoreEvent>>() {
      @Override
      public Optional<MetaStoreEvent> answer(InvocationOnMock invocation) throws Throwable {
        WINDOW_UNITS.sleep(WINDOW + 1);
        return Optional.of(events.get(0));
      }
    });
    Optional<MetaStoreEvent> next = aggregatingMessageReader.read();
    assertThat(next.get()).isEqualTo(events.get(0));
    verify(delegate).read();
    verify(aggregator).aggregate(events);
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
    when(delegate.read()).thenAnswer(new Answer<Optional<MetaStoreEvent>>() {
      @Override
      public Optional<MetaStoreEvent> answer(InvocationOnMock invocation) throws Throwable {
        WINDOW_UNITS.sleep(counter[0] < events.length - 1 ? 1 : WINDOW);
        return Optional.of(events[counter[0]++]);
      }
    });
    Optional<MetaStoreEvent> next = aggregatingMessageReader.read();
    assertThat(next.get()).isEqualTo(events[0]);
    verify(delegate, atLeast(2)).read();
    verify(aggregator).aggregate(any());
    int numOfEventsCaptured = buffer.size();
    assertThat(buffer).containsAll(Arrays.asList(Arrays.copyOfRange(events, 1, numOfEventsCaptured)));
  }

  @Test
  public void failuresInDelegateArePropagated() {
    SerDeException e = new SerDeException("oops");
    expectedException.expect(is(e));
    when(delegate.read()).thenThrow(e);
    aggregatingMessageReader.read();
  }

  @Test
  public void emptyAggregate() {
    when(delegate.read()).thenAnswer(new Answer<Optional<MetaStoreEvent>>() {
      @Override
      public Optional<MetaStoreEvent> answer(InvocationOnMock invocation) throws Throwable {
        WINDOW_UNITS.sleep(WINDOW + 1);
        return Optional.empty();
      }
    });

    Optional<MetaStoreEvent> event = aggregatingMessageReader.read();
    assertThat(event).isEqualTo(Optional.empty());
  }

}
