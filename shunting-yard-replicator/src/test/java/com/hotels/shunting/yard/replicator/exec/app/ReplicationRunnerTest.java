package com.hotels.shunting.yard.replicator.exec.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static com.hotels.shunting.yard.common.event.EventType.ON_ADD_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_ALTER_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_ALTER_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.ON_CREATE_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_PARTITION;
import static com.hotels.shunting.yard.common.event.EventType.ON_DROP_TABLE;
import static com.hotels.shunting.yard.common.event.EventType.ON_INSERT;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.ApplicationArguments;

import com.hotels.shunting.yard.common.event.SerializableAddPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterTableEvent;
import com.hotels.shunting.yard.common.event.SerializableCreateTableEvent;
import com.hotels.shunting.yard.common.event.SerializableDropPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropTableEvent;
import com.hotels.shunting.yard.common.event.SerializableInsertEvent;
import com.hotels.shunting.yard.common.messaging.MessageReader;
import com.hotels.shunting.yard.common.receiver.ShuntingYardMetaStoreEventListener;

@RunWith(MockitoJUnitRunner.class)
public class ReplicationRunnerTest {

  private @Mock ApplicationArguments args;
  private @Mock MessageReader messageReader;
  private @Mock ShuntingYardMetaStoreEventListener listener;

  private ReplicationRunner runner;

  @Before
  public void init() {
    when(messageReader.hasNext()).thenReturn(true, false);
    runner = new ReplicationRunner(messageReader, listener);
  }

  @Test
  public void exitCode() {
    assertThat(runner.getExitCode()).isEqualTo(0);
  }

  @Test
  public void onCreateTable() {
    SerializableCreateTableEvent event = mock(SerializableCreateTableEvent.class);
    when(event.getEventType()).thenReturn(ON_CREATE_TABLE);
    when(messageReader.next()).thenReturn(event);
    runner.run(args);
    verify(listener).onCreateTable(event);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void onAlterTable() {
    SerializableAlterTableEvent event = mock(SerializableAlterTableEvent.class);
    when(event.getEventType()).thenReturn(ON_ALTER_TABLE);
    when(messageReader.next()).thenReturn(event);
    runner.run(args);
    verify(listener).onAlterTable(event);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void onDropTable() {
    SerializableDropTableEvent event = mock(SerializableDropTableEvent.class);
    when(event.getEventType()).thenReturn(ON_DROP_TABLE);
    when(messageReader.next()).thenReturn(event);
    runner.run(args);
    verify(listener).onDropTable(event);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void onAddPartition() {
    SerializableAddPartitionEvent event = mock(SerializableAddPartitionEvent.class);
    when(event.getEventType()).thenReturn(ON_ADD_PARTITION);
    when(messageReader.next()).thenReturn(event);
    runner.run(args);
    verify(listener).onAddPartition(event);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void onAlterPartition() {
    SerializableAlterPartitionEvent event = mock(SerializableAlterPartitionEvent.class);
    when(event.getEventType()).thenReturn(ON_ALTER_PARTITION);
    when(messageReader.next()).thenReturn(event);
    runner.run(args);
    verify(listener).onAlterPartition(event);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void onDropPartition() {
    SerializableDropPartitionEvent event = mock(SerializableDropPartitionEvent.class);
    when(event.getEventType()).thenReturn(ON_DROP_PARTITION);
    when(messageReader.next()).thenReturn(event);
    runner.run(args);
    verify(listener).onDropPartition(event);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void onInsert() {
    SerializableInsertEvent event = mock(SerializableInsertEvent.class);
    when(event.getEventType()).thenReturn(ON_INSERT);
    when(messageReader.next()).thenReturn(event);
    runner.run(args);
    verify(listener).onInsert(event);
    verifyNoMoreInteractions(listener);
  }

}
