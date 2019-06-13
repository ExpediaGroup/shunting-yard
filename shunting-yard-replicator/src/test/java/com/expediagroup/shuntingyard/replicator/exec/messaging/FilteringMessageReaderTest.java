/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.expediagroup.shuntingyard.replicator.exec.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.expediagroup.shuntingyard.replicator.exec.messaging.FilteringMessageReader;
import com.expediagroup.shuntingyard.replicator.exec.receiver.TableSelector;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;

@RunWith(MockitoJUnitRunner.class)
public class FilteringMessageReaderTest {

  private static final String DB_NAME = "test_db";
  private static final String TABLE_NAME1 = "test_table1";
  private static final String TABLE_NAME2 = "test_table2";
  private static final String TABLE_NAME3 = "test_table3";

  private @Mock MessageEvent messageEvent1;
  private @Mock MessageEvent messageEvent2;
  private @Mock MessageEvent messageEvent3;
  private @Mock ListenerEvent listenerEvent1;
  private @Mock ListenerEvent listenerEvent2;
  private @Mock ListenerEvent listenerEvent3;
  private @Mock MessageReader delegate;
  private @Mock TableSelector tableSelector;
  private FilteringMessageReader filteringMessageReader;

  @Before
  public void init() {
    when(messageEvent1.getEvent()).thenReturn(listenerEvent1);
    when(messageEvent2.getEvent()).thenReturn(listenerEvent2);
    when(messageEvent3.getEvent()).thenReturn(listenerEvent3);
    when(listenerEvent1.getDbName()).thenReturn(DB_NAME);
    when(listenerEvent1.getTableName()).thenReturn(TABLE_NAME1);
    when(listenerEvent2.getDbName()).thenReturn(DB_NAME);
    when(listenerEvent2.getTableName()).thenReturn(TABLE_NAME2);
    when(listenerEvent3.getDbName()).thenReturn(DB_NAME);
    when(listenerEvent3.getTableName()).thenReturn(TABLE_NAME3);

    when(delegate.read())
        .thenReturn(Optional.of(messageEvent1))
        .thenReturn(Optional.of(messageEvent2))
        .thenReturn(Optional.of(messageEvent3));
  }

  @Test
  public void selectFirstAndThirdEventButSkipSecond() {
    when(tableSelector.canProcess(listenerEvent1)).thenReturn(true);
    when(tableSelector.canProcess(listenerEvent2)).thenReturn(false);
    when(tableSelector.canProcess(listenerEvent3)).thenReturn(true);

    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);

    ListenerEvent event = filteringMessageReader.read().get().getEvent();
    assertThat(event.getDbName()).isEqualTo(DB_NAME);
    assertThat(event.getTableName()).isEqualTo(TABLE_NAME1);

    Optional<MessageEvent> filtered = filteringMessageReader.read();
    assertThat(filtered).isEqualTo(Optional.empty());

    event = filteringMessageReader.read().get().getEvent();
    assertThat(event.getDbName()).isEqualTo(DB_NAME);
    assertThat(event.getTableName()).isEqualTo(TABLE_NAME3);
    verify(delegate).delete(messageEvent2);
  }

  @Test
  public void skipFirstEventButSelectSecondAndThird() {
    when(tableSelector.canProcess(listenerEvent1)).thenReturn(false);
    when(tableSelector.canProcess(listenerEvent2)).thenReturn(true);
    when(tableSelector.canProcess(listenerEvent3)).thenReturn(true);

    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);

    Optional<MessageEvent> filtered = filteringMessageReader.read();
    assertThat(filtered).isEqualTo(Optional.empty());

    ListenerEvent event = filteringMessageReader.read().get().getEvent();
    assertThat(event.getDbName()).isEqualTo(DB_NAME);
    assertThat(event.getTableName()).isEqualTo(TABLE_NAME2);

    event = filteringMessageReader.read().get().getEvent();
    assertThat(event.getDbName()).isEqualTo(DB_NAME);
    assertThat(event.getTableName()).isEqualTo(TABLE_NAME3);
    verify(delegate).delete(messageEvent1);
  }

  @Test
  public void emptyDelegateReader() {
    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);
    assertThat(filteringMessageReader.read()).isEqualTo(Optional.empty());
    verify(delegate).delete(messageEvent1);
  }

  @Test
  public void typicalDelete() {
    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);
    filteringMessageReader.delete(messageEvent1);
    verify(delegate).delete(messageEvent1);
  }

  @Test
  public void deleteThrowsException() {
    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);
    doThrow(new RuntimeException()).when(delegate)
        .delete(any());
    filteringMessageReader.delete(messageEvent1);
    verify(delegate).delete(messageEvent1);
  }

  @Test
  public void typicalClose() throws IOException {
    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);
    filteringMessageReader.close();
    verify(delegate).close();
  }

}
