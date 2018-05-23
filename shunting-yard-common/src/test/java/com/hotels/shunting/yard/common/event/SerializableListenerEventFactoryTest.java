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
package com.hotels.shunting.yard.common.event;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hive.common.util.HiveVersionInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class SerializableListenerEventFactoryTest {

  private static final String METASTORE_URIS = "thrift://localhost:1234";

  private @Mock Iterator<Partition> partitionIterator;

  private Map<String, String> parameters;
  private SerializableListenerEventFactory factory;

  @Before
  public void init() {
    parameters = new HashMap<>();
    HiveConf config = new HiveConf();
    config.setVar(METASTOREURIS, METASTORE_URIS);
    factory = new SerializableListenerEventFactory(config);
  }

  private <T extends ListenerEvent> T mockEvent(Class<T> clazz) {
    T event = mock(clazz);
    when(event.getStatus()).thenReturn(true);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        parameters.put(invocation.getArgument(0).toString(), invocation.getArgument(1).toString());
        return null;
      }
    }).when(event).putParameter(anyString(), anyString());
    return event;
  }

  private void assertCommon(SerializableListenerEvent event) {
    assertThat(event.getStatus()).isTrue();
    // We don't use event.getParameters() here because is being deferred to parameters in the stub
    assertThat(parameters).containsEntry(METASTOREURIS.varname, METASTORE_URIS).containsEntry(
        CustomEventParameters.HIVE_VERSION.varname(), HiveVersionInfo.getVersion());
  }

  @Test
  public void createSerializableCreateTableEvent() {
    CreateTableEvent event = mockEvent(CreateTableEvent.class);
    SerializableListenerEvent serializableEvent = factory.create(event);
    assertCommon(serializableEvent);
    assertThat(serializableEvent.getEventType()).isSameAs(EventType.ON_CREATE_TABLE);
  }

  @Test
  public void createSerializableAlterTableEvent() {
    AlterTableEvent event = mockEvent(AlterTableEvent.class);
    SerializableListenerEvent serializableEvent = factory.create(event);
    assertCommon(serializableEvent);
    assertThat(serializableEvent.getEventType()).isSameAs(EventType.ON_ALTER_TABLE);
  }

  @Test
  public void createSerializableDropTableEvent() {
    DropTableEvent event = mockEvent(DropTableEvent.class);
    SerializableListenerEvent serializableEvent = factory.create(event);
    assertCommon(serializableEvent);
    assertThat(serializableEvent.getEventType()).isSameAs(EventType.ON_DROP_TABLE);
  }

  @Test
  public void createSerializableAddPartitionEvent() {
    AddPartitionEvent event = mockEvent(AddPartitionEvent.class);
    when(event.getPartitionIterator()).thenReturn(partitionIterator);
    SerializableListenerEvent serializableEvent = factory.create(event);
    assertCommon(serializableEvent);
    assertThat(serializableEvent.getEventType()).isSameAs(EventType.ON_ADD_PARTITION);
  }

  @Test
  public void createSerializableAlterPartitionEvent() {
    AlterPartitionEvent event = mockEvent(AlterPartitionEvent.class);
    SerializableListenerEvent serializableEvent = factory.create(event);
    assertCommon(serializableEvent);
    assertThat(serializableEvent.getEventType()).isSameAs(EventType.ON_ALTER_PARTITION);
  }

  @Test
  public void createSerializableDropPartitionEvent() {
    DropPartitionEvent event = mockEvent(DropPartitionEvent.class);
    when(event.getPartitionIterator()).thenReturn(partitionIterator);
    SerializableListenerEvent serializableEvent = factory.create(event);
    assertCommon(serializableEvent);
    assertThat(serializableEvent.getEventType()).isSameAs(EventType.ON_DROP_PARTITION);
  }

  @Test
  public void createSerializableInsertEvent() {
    InsertEvent event = mockEvent(InsertEvent.class);
    SerializableListenerEvent serializableEvent = factory.create(event);
    assertCommon(serializableEvent);
    assertThat(serializableEvent.getEventType()).isSameAs(EventType.ON_INSERT);
  }

}
