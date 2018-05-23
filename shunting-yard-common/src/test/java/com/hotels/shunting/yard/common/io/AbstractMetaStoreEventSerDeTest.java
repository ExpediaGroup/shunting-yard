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
package com.hotels.shunting.yard.common.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.hotels.shunting.yard.common.io.SerDeTestUtils.DATABASE;
import static com.hotels.shunting.yard.common.io.SerDeTestUtils.TABLE;
import static com.hotels.shunting.yard.common.io.SerDeTestUtils.createEnvironmentContext;
import static com.hotels.shunting.yard.common.io.SerDeTestUtils.createPartition;
import static com.hotels.shunting.yard.common.io.SerDeTestUtils.createTable;

import java.util.Arrays;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.hotels.shunting.yard.common.event.SerializableAddPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableAlterTableEvent;
import com.hotels.shunting.yard.common.event.SerializableCreateTableEvent;
import com.hotels.shunting.yard.common.event.SerializableDropPartitionEvent;
import com.hotels.shunting.yard.common.event.SerializableDropTableEvent;
import com.hotels.shunting.yard.common.event.SerializableInsertEvent;
import com.hotels.shunting.yard.common.event.SerializableListenerEvent;

@RunWith(Parameterized.class)
public abstract class AbstractMetaStoreEventSerDeTest {

  private static InsertEventRequestData mockInsertEventRequestData() {
    InsertEventRequestData eventRequestData = mock(InsertEventRequestData.class);
    when(eventRequestData.getFilesAdded()).thenReturn(Arrays.asList("file_0000"));
    return eventRequestData;
  }

  private static HMSHandler mockHandler() throws Exception {
    GetTableResult getTableResult = mock(GetTableResult.class);
    when(getTableResult.getTable()).thenReturn(createTable());
    HMSHandler handler = mock(HMSHandler.class);
    when(handler.get_table_req(any(GetTableRequest.class))).thenReturn(getTableResult);
    return handler;
  }

  private static SerializableCreateTableEvent serializableCreateTableEvent() throws Exception {
    CreateTableEvent event = new CreateTableEvent(createTable(), true, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new SerializableCreateTableEvent(event);
  }

  private static SerializableAlterTableEvent serializableAlterTableEvent() throws Exception {
    AlterTableEvent event = new AlterTableEvent(createTable(), createTable(new FieldSchema("new_col", "string", null)),
        true, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new SerializableAlterTableEvent(event);
  }

  private static SerializableDropTableEvent serializableDropTableEvent() throws Exception {
    DropTableEvent event = new DropTableEvent(createTable(), true, false, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new SerializableDropTableEvent(event);
  }

  private static SerializableAddPartitionEvent serializableAddPartitionEvent() throws Exception {
    AddPartitionEvent event = new AddPartitionEvent(createTable(), createPartition("a"), true, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new SerializableAddPartitionEvent(event);
  }

  private static SerializableAlterPartitionEvent serializableAlterPartitionEvent() throws Exception {
    AlterPartitionEvent event = new AlterPartitionEvent(createPartition("a"), createPartition("b"), createTable(), true,
        mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new SerializableAlterPartitionEvent(event);
  }

  private static SerializableDropPartitionEvent serializableDropPartitionEvent() throws Exception {
    DropPartitionEvent event = new DropPartitionEvent(createTable(), createPartition("a"), true, false, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new SerializableDropPartitionEvent(event);
  }

  private static SerializableInsertEvent serializableInsertEvent() throws Exception {
    InsertEvent event = new InsertEvent(DATABASE, TABLE, Arrays.asList("a"), mockInsertEventRequestData(), true,
        mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new SerializableInsertEvent(event);
  }

  @Parameters(name = "{index}: {0}")
  public static SerializableListenerEvent[] data() throws Exception {
    return new SerializableListenerEvent[] {
        serializableCreateTableEvent(),
        serializableAlterTableEvent(),
        serializableDropTableEvent(),
        serializableAddPartitionEvent(),
        serializableAlterPartitionEvent(),
        serializableDropPartitionEvent(),
        serializableInsertEvent() };
  }

  public @Parameter SerializableListenerEvent event;

  protected abstract MetaStoreEventSerDe serDe();

  @Test
  public void typical() throws Exception {
    SerializableListenerEvent processedEvent = serDe().unmarshal(serDe().marshal(event));
    assertThat(processedEvent).isNotSameAs(event).isEqualTo(event);
  }

}
