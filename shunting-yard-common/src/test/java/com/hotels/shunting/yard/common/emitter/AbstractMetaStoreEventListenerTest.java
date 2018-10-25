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
package com.hotels.shunting.yard.common.emitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.event.SerializableListenerEventFactory;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAddPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAlterTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryCreateTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryInsertTableEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.messaging.Message;
import com.hotels.shunting.yard.common.messaging.MessageTask;
import com.hotels.shunting.yard.common.messaging.MessageTaskFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest(EmitterUtils.class)
public class AbstractMetaStoreEventListenerTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final byte[] PAYLOAD = "payload".getBytes();

  private @Mock MetaStoreEventSerDe eventSerDe;
  private @Mock MessageTask messageTask;
  private @Mock MessageTaskFactory messageTaskFactory;
  private @Mock SerializableListenerEventFactory serializableListenerEventFactory;
  private @Mock ExecutorService executorService;

  private @Captor ArgumentCaptor<MessageTask> captor;

  private final Configuration config = new Configuration();
  private AbstractMetaStoreEventListener listener;

  @Before
  public void init() throws Exception {
    mockStatic(EmitterUtils.class);
    when(eventSerDe.marshal(any(SerializableListenerEvent.class))).thenReturn(PAYLOAD);
    when(messageTaskFactory.newTask(any(Message.class))).thenReturn(messageTask);
    listener = new AbstractMetaStoreEventListener(config, serializableListenerEventFactory, executorService) {
      @Override
      protected MetaStoreEventSerDe getMetaStoreEventSerDe() {
        return eventSerDe;
      }

      @Override
      protected MessageTaskFactory getMessageTaskFactory() {
        return messageTaskFactory;
      }
    };
  }

  @Test
  public void onCreateTable() throws Exception {
    CreateTableEvent event = mock(CreateTableEvent.class);
    SerializableApiaryCreateTableEvent serializableApiaryEvent = mock(SerializableApiaryCreateTableEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    listener.onCreateTable(event);
    verify(executorService).submit(captor.capture());
    assertThat(captor.getValue()).isEqualTo(new WrappingMessageTask(messageTask));
  }

  @Test
  public void onCreateTableErrors() throws Exception {
    CreateTableEvent event = mock(CreateTableEvent.class);
    SerializableApiaryCreateTableEvent serializableApiaryEvent = mock(SerializableApiaryCreateTableEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    RuntimeException e = new RuntimeException("Something has gone wrong");
    doThrow(e).when(executorService).submit(any(MessageTask.class));
    listener.onCreateTable(event);
    verifyStatic(EmitterUtils.class);
    EmitterUtils.error(e);
  }

  @Test
  public void onAlterTable() throws Exception {
    AlterTableEvent event = mock(AlterTableEvent.class);
    SerializableApiaryAlterTableEvent serializableApiaryEvent = mock(SerializableApiaryAlterTableEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    listener.onAlterTable(event);
    verify(executorService).submit(captor.capture());
    assertThat(captor.getValue()).isEqualTo(new WrappingMessageTask(messageTask));
  }

  @Test
  public void onAlterTableErrors() throws Exception {
    AlterTableEvent event = mock(AlterTableEvent.class);
    SerializableApiaryAlterTableEvent serializableApiaryEvent = mock(SerializableApiaryAlterTableEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    RuntimeException e = new RuntimeException("Something has gone wrong");
    doThrow(e).when(executorService).submit(any(MessageTask.class));
    listener.onAlterTable(event);
    verifyStatic(EmitterUtils.class);
    EmitterUtils.error(e);
  }

  @Test
  public void onDropTable() throws Exception {
    DropTableEvent event = mock(DropTableEvent.class);
    SerializableApiaryDropTableEvent serializableApiaryEvent = mock(SerializableApiaryDropTableEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    listener.onDropTable(event);
    verify(executorService).submit(captor.capture());
    assertThat(captor.getValue()).isEqualTo(new WrappingMessageTask(messageTask));
  }

  @Test
  public void onDropTableErrors() throws Exception {
    DropTableEvent event = mock(DropTableEvent.class);
    SerializableApiaryDropTableEvent serializableApiaryEvent = mock(SerializableApiaryDropTableEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    RuntimeException e = new RuntimeException("Something has gone wrong");
    doThrow(e).when(executorService).submit(any(MessageTask.class));
    listener.onDropTable(event);
    verifyStatic(EmitterUtils.class);
    EmitterUtils.error(e);
  }

  @Test
  public void onAddPartition() throws Exception {
    AddPartitionEvent event = mock(AddPartitionEvent.class);
    SerializableApiaryAddPartitionEvent serializableApiaryEvent = mock(SerializableApiaryAddPartitionEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    listener.onAddPartition(event);
    verify(executorService).submit(captor.capture());
    assertThat(captor.getValue()).isEqualTo(new WrappingMessageTask(messageTask));
  }

  @Test
  public void onAddPartitionErrors() throws Exception {
    AddPartitionEvent event = mock(AddPartitionEvent.class);
    SerializableApiaryAddPartitionEvent serializableApiaryEvent = mock(SerializableApiaryAddPartitionEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    RuntimeException e = new RuntimeException("Something has gone wrong");
    doThrow(e).when(executorService).submit(any(MessageTask.class));
    listener.onAddPartition(event);
    verifyStatic(EmitterUtils.class);
    EmitterUtils.error(e);
  }

  @Test
  public void onAlterPartition() throws Exception {
    AlterPartitionEvent event = mock(AlterPartitionEvent.class);
    SerializableApiaryAlterPartitionEvent serializableApiaryEvent = mock(SerializableApiaryAlterPartitionEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    listener.onAlterPartition(event);
    verify(executorService).submit(captor.capture());
    assertThat(captor.getValue()).isEqualTo(new WrappingMessageTask(messageTask));
  }

  @Test
  public void onAlterPartitionErrors() throws Exception {
    AlterPartitionEvent event = mock(AlterPartitionEvent.class);
    SerializableApiaryAlterPartitionEvent serializableApiaryEvent = mock(SerializableApiaryAlterPartitionEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    RuntimeException e = new RuntimeException("Something has gone wrong");
    doThrow(e).when(executorService).submit(any(MessageTask.class));
    listener.onAlterPartition(event);
    verifyStatic(EmitterUtils.class);
    EmitterUtils.error(e);
  }

  @Test
  public void onDropPartition() throws Exception {
    DropPartitionEvent event = mock(DropPartitionEvent.class);
    SerializableApiaryDropPartitionEvent serializableApiaryEvent = mock(SerializableApiaryDropPartitionEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    listener.onDropPartition(event);
    verify(executorService).submit(captor.capture());
    assertThat(captor.getValue()).isEqualTo(new WrappingMessageTask(messageTask));
  }

  @Test
  public void onDropPartitionErrors() throws Exception {
    DropPartitionEvent event = mock(DropPartitionEvent.class);
    SerializableApiaryDropPartitionEvent serializableApiaryEvent = mock(SerializableApiaryDropPartitionEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    RuntimeException e = new RuntimeException("Something has gone wrong");
    doThrow(e).when(executorService).submit(any(MessageTask.class));
    listener.onDropPartition(event);
    verifyStatic(EmitterUtils.class);
    EmitterUtils.error(e);
  }

  @Test
  public void onInsert() throws Exception {
    InsertEvent event = mock(InsertEvent.class);
    SerializableApiaryInsertTableEvent serializableApiaryEvent = mock(SerializableApiaryInsertTableEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    listener.onInsert(event);
    verify(executorService).submit(captor.capture());
    assertThat(captor.getValue()).isEqualTo(new WrappingMessageTask(messageTask));
  }

  @Test
  public void onInsertErrors() throws Exception {
    InsertEvent event = mock(InsertEvent.class);
    SerializableApiaryInsertTableEvent serializableApiaryEvent = mock(SerializableApiaryInsertTableEvent.class);
    when(serializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(serializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableApiaryEvent);
    RuntimeException e = new RuntimeException("Something has gone wrong");
    doThrow(e).when(executorService).submit(any(MessageTask.class));
    listener.onInsert(event);
    verifyStatic(EmitterUtils.class);
    EmitterUtils.error(e);
  }

  @Test
  public void onConfigChange() throws Exception {
    listener.onConfigChange(mock(ConfigChangeEvent.class));
    verify(executorService, never()).submit(any(Runnable.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onCreateDatabase() throws Exception {
    listener.onCreateDatabase(mock(CreateDatabaseEvent.class));
    verify(executorService, never()).submit(any(Runnable.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onDropDatabase() throws Exception {
    listener.onDropDatabase(mock(DropDatabaseEvent.class));
    verify(executorService, never()).submit(any(Runnable.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onLoadPartitionDone() throws Exception {
    listener.onLoadPartitionDone(mock(LoadPartitionDoneEvent.class));
    verify(executorService, never()).submit(any(Runnable.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onAddIndex() throws Exception {
    listener.onAddIndex(mock(AddIndexEvent.class));
    verify(executorService, never()).submit(any(Runnable.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onDropIndex() throws Exception {
    listener.onDropIndex(mock(DropIndexEvent.class));
    verify(executorService, never()).submit(any(Runnable.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onAlterIndex() throws Exception {
    listener.onAlterIndex(mock(AlterIndexEvent.class));
    verify(executorService, never()).submit(any(Runnable.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onCreateFunction() throws Exception {
    listener.onCreateFunction(mock(CreateFunctionEvent.class));
    verify(executorService, never()).submit(any(Runnable.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onDropFunction() throws Exception {
    listener.onDropFunction(mock(DropFunctionEvent.class));
    verify(executorService, never()).submit(any(Runnable.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

}
