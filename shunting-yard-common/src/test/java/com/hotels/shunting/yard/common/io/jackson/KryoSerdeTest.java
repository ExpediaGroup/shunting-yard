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
package com.hotels.shunting.yard.common.io.jackson;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.hotels.shunting.yard.common.io.SerDeTestUtils.createEnvironmentContext;
import static com.hotels.shunting.yard.common.io.SerDeTestUtils.createTable;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KryoSerdeTest {

  private static HMSHandler mockHandler() throws Exception {
    GetTableResult getTableResult = mock(GetTableResult.class);
    when(getTableResult.getTable()).thenReturn(createTable());
    HMSHandler handler = mock(HMSHandler.class);
    when(handler.get_table_req(any(GetTableRequest.class))).thenReturn(getTableResult);
    return handler;
  }

  private static CreateTableEvent createTableEvent() throws Exception {
    CreateTableEvent event = new CreateTableEvent(createTable(), true, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return event;
  }

  @Test
  public void typical() throws Exception {
    KryoMetaStoreEventSerDe serde = new KryoMetaStoreEventSerDe();

    ListenerEvent processedEvent = serde.unmarshal(serde.marshal(createTableEvent()));

    System.out.println(processedEvent.getStatus());

    // assertThat(processedEvent).isNotSameAs(event).isEqualTo(event);
  }
}
