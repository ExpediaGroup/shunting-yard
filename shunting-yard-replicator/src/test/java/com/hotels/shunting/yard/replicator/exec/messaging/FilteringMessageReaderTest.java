package com.hotels.shunting.yard.replicator.exec.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.shunting.yard.common.event.ListenerEvent;
import com.hotels.shunting.yard.receiver.sqs.messaging.SqsMessageReader;
import com.hotels.shunting.yard.replicator.exec.receiver.TableSelector;

@RunWith(MockitoJUnitRunner.class)
public class FilteringMessageReaderTest {

  private static final String DB_NAME = "test_db";
  private static final String TABLE_NAME1 = "test_table1";
  private static final String TABLE_NAME2 = "test_table2";
  private static final String TABLE_NAME3 = "test_table3";

  private @Mock ListenerEvent listenerEvent1;
  private @Mock ListenerEvent listenerEvent2;
  private @Mock ListenerEvent listenerEvent3;
  private @Mock SqsMessageReader delegate;
  private @Mock TableSelector tableSelector;
  private FilteringMessageReader filteringMessageReader;

  @Before
  public void init() {
    when(listenerEvent1.getDbName()).thenReturn(DB_NAME);
    when(listenerEvent1.getTableName()).thenReturn(TABLE_NAME1);
    when(listenerEvent2.getDbName()).thenReturn(DB_NAME);
    when(listenerEvent2.getTableName()).thenReturn(TABLE_NAME2);
    when(listenerEvent3.getDbName()).thenReturn(DB_NAME);
    when(listenerEvent3.getTableName()).thenReturn(TABLE_NAME3);

    when(delegate.hasNext()).thenReturn(true);
    when(delegate.next()).thenReturn(listenerEvent1).thenReturn(listenerEvent2).thenReturn(listenerEvent3);
  }

  @Test
  public void selectFirstAndThirdEventButSkipSecond() {
    when(tableSelector.canProcess(listenerEvent1)).thenReturn(true);
    when(tableSelector.canProcess(listenerEvent2)).thenReturn(false);
    when(tableSelector.canProcess(listenerEvent3)).thenReturn(true);

    List<ListenerEvent> expectedEventList = new ArrayList<>();
    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);
    ListenerEvent event = null;

    for (int i = 0; i < 2; i++) {
      if (filteringMessageReader.hasNext()) {
        event = filteringMessageReader.next();
        expectedEventList.add(event);
      }
    }
    assertThat(expectedEventList.get(0).getDbName()).isEqualTo(DB_NAME);
    assertThat(expectedEventList.get(0).getTableName()).isEqualTo(TABLE_NAME1);

    assertThat(expectedEventList.get(1).getDbName()).isEqualTo(DB_NAME);
    assertThat(expectedEventList.get(1).getTableName()).isEqualTo(TABLE_NAME3);
  }

  @Test
  public void skipFirstEventButSelectSecondAndThird() {
    when(tableSelector.canProcess(listenerEvent1)).thenReturn(false);
    when(tableSelector.canProcess(listenerEvent2)).thenReturn(true);
    when(tableSelector.canProcess(listenerEvent3)).thenReturn(true);

    List<ListenerEvent> expectedEventList = new ArrayList<>();
    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);
    ListenerEvent event = null;

    for (int i = 0; i < 2; i++) {
      if (filteringMessageReader.hasNext()) {
        event = filteringMessageReader.next();
        expectedEventList.add(event);
      }
    }
    assertThat(expectedEventList.get(0).getDbName()).isEqualTo(DB_NAME);
    assertThat(expectedEventList.get(0).getTableName()).isEqualTo(TABLE_NAME2);

    assertThat(expectedEventList.get(1).getDbName()).isEqualTo(DB_NAME);
    assertThat(expectedEventList.get(1).getTableName()).isEqualTo(TABLE_NAME3);
  }

}
