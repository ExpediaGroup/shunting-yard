package com.hotels.shunting.yard.replicator.exec.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.shunting.yard.common.event.ListenerEvent;
import com.hotels.shunting.yard.common.messaging.MessageReader;
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
  private @Mock MessageReader delegate;
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

    when(delegate.next()).thenReturn(listenerEvent1).thenReturn(listenerEvent2).thenReturn(listenerEvent3);
  }

  @Test
  public void selectFirstAndThirdEventButSkipSecond() {
    when(delegate.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
    when(tableSelector.canProcess(listenerEvent1)).thenReturn(true);
    when(tableSelector.canProcess(listenerEvent2)).thenReturn(false);
    when(tableSelector.canProcess(listenerEvent3)).thenReturn(true);

    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);

    assertThat(filteringMessageReader.hasNext(), is(true));
    ListenerEvent event = filteringMessageReader.next();
    assertThat(event.getDbName()).isEqualTo(DB_NAME);
    assertThat(event.getTableName()).isEqualTo(TABLE_NAME1);

    assertThat(filteringMessageReader.hasNext(), is(true));
    event = filteringMessageReader.next();
    assertThat(event.getDbName()).isEqualTo(DB_NAME);
    assertThat(event.getTableName()).isEqualTo(TABLE_NAME3);

    assertThat(filteringMessageReader.hasNext(), is(false));
  }

  @Test
  public void skipFirstEventButSelectSecondAndThird() {
    when(delegate.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
    when(tableSelector.canProcess(listenerEvent1)).thenReturn(false);
    when(tableSelector.canProcess(listenerEvent2)).thenReturn(true);
    when(tableSelector.canProcess(listenerEvent3)).thenReturn(true);

    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);

    assertThat(filteringMessageReader.hasNext(), is(true));
    ListenerEvent event = filteringMessageReader.next();
    assertThat(event.getDbName()).isEqualTo(DB_NAME);
    assertThat(event.getTableName()).isEqualTo(TABLE_NAME2);

    assertThat(filteringMessageReader.hasNext(), is(true));
    event = filteringMessageReader.next();
    assertThat(event.getDbName()).isEqualTo(DB_NAME);
    assertThat(event.getTableName()).isEqualTo(TABLE_NAME3);

    assertThat(filteringMessageReader.hasNext(), is(false));
  }

  @Test
  public void emptyDelegateReader() {
    when(delegate.hasNext()).thenReturn(false);
    filteringMessageReader = new FilteringMessageReader(delegate, tableSelector);

    assertThat(filteringMessageReader.hasNext(), is(false));
  }

}
