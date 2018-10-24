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
package com.hotels.shunting.yard.emitter.sqs.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

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

@RunWith(MockitoJUnitRunner.class)
public class SqsMetaStoreEventListenerTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final byte[] PAYLOAD = "payload".getBytes();

  private @Mock MetaStoreEventSerDe eventSerDe;
  private @Mock MessageTask messageTask;
  private @Mock MessageTaskFactory messageTaskFactory;
  private @Mock SerializableListenerEventFactory SerializableListenerEventFactory;
  private @Mock ExecutorService executorService;

  private final Configuration config = new Configuration();
  private SqsMetaStoreEventListener listener;

  @Before
  public void init() throws Exception {
    when(eventSerDe.marshal(any(SerializableListenerEvent.class))).thenReturn(PAYLOAD);
    when(messageTaskFactory.newTask(any(Message.class))).thenReturn(messageTask);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ((Runnable) invocation.getArgument(0)).run();
        return null;
      }
    }).when(executorService).submit(any(Runnable.class));
    listener = new SqsMetaStoreEventListener(config, SerializableListenerEventFactory, eventSerDe, messageTaskFactory,
        executorService);
  }

  @Test
  public void onCreateTable() throws Exception {
    CreateTableEvent event = mock(CreateTableEvent.class);
    SerializableApiaryCreateTableEvent SerializableApiaryEvent = mock(SerializableApiaryCreateTableEvent.class);
    when(SerializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(SerializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(SerializableListenerEventFactory.create(event)).thenReturn(SerializableApiaryEvent);
    listener.onCreateTable(event);
    verify(messageTask).run();
  }

  @Test
  public void onAlterTable() throws Exception {
    AlterTableEvent event = mock(AlterTableEvent.class);
    SerializableApiaryAlterTableEvent SerializableApiaryEvent = mock(SerializableApiaryAlterTableEvent.class);
    when(SerializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(SerializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(SerializableListenerEventFactory.create(event)).thenReturn(SerializableApiaryEvent);
    listener.onAlterTable(event);
    verify(messageTask).run();
  }

  @Test
  public void onDropTable() throws Exception {
    DropTableEvent event = mock(DropTableEvent.class);
    SerializableApiaryDropTableEvent SerializableApiaryEvent = mock(SerializableApiaryDropTableEvent.class);
    when(SerializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(SerializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(SerializableListenerEventFactory.create(event)).thenReturn(SerializableApiaryEvent);
    listener.onDropTable(event);
    verify(messageTask).run();
  }

  @Test
  public void onAddPartition() throws Exception {
    AddPartitionEvent event = mock(AddPartitionEvent.class);
    SerializableApiaryAddPartitionEvent SerializableApiaryEvent = mock(SerializableApiaryAddPartitionEvent.class);
    when(SerializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(SerializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(SerializableListenerEventFactory.create(event)).thenReturn(SerializableApiaryEvent);
    listener.onAddPartition(event);
    verify(messageTask).run();
  }

  @Test
  public void onAlterPartition() throws Exception {
    AlterPartitionEvent event = mock(AlterPartitionEvent.class);
    SerializableApiaryAlterPartitionEvent SerializableApiaryEvent = mock(SerializableApiaryAlterPartitionEvent.class);
    when(SerializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(SerializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(SerializableListenerEventFactory.create(event)).thenReturn(SerializableApiaryEvent);
    listener.onAlterPartition(event);
    verify(messageTask).run();
  }

  @Test
  public void onDropPartition() throws Exception {
    DropPartitionEvent event = mock(DropPartitionEvent.class);
    SerializableApiaryDropPartitionEvent SerializableApiaryEvent = mock(SerializableApiaryDropPartitionEvent.class);
    when(SerializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(SerializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(SerializableListenerEventFactory.create(event)).thenReturn(SerializableApiaryEvent);
    listener.onDropPartition(event);
    verify(messageTask).run();
  }

  @Test
  public void onInsert() throws Exception {
    InsertEvent event = mock(InsertEvent.class);
    SerializableApiaryInsertTableEvent SerializableApiaryEvent = mock(SerializableApiaryInsertTableEvent.class);
    when(SerializableApiaryEvent.getDbName()).thenReturn(DATABASE);
    when(SerializableApiaryEvent.getTableName()).thenReturn(TABLE);
    when(SerializableListenerEventFactory.create(event)).thenReturn(SerializableApiaryEvent);
    listener.onInsert(event);
    verify(messageTask).run();
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
