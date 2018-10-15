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
package com.hotels.shunting.yard.receiver.sqs.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.collect.ImmutableList;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAddPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryCreateTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryInsertTableEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.jackson.JsonMetaStoreEventSerDe;

@RunWith(MockitoJUnitRunner.class)
public class SerdeWithJsonInputTest {

  private final MetaStoreEventSerDe serDe = new JsonMetaStoreEventSerDe();
  private final static String ADD_PARTITION_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"ADD_PARTITION\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"partition\":[\"col_1\", \"col_2\", \"col_3\"],\"sourceMetastoreUris\":\"thrift://host:9083\"}";
  private final static String ALTER_PARTITION_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"ALTER_PARTITION\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"partition\":[\"col_1\", \"col_2\", \"col_3\"],\"oldPartition\": [\"col_1\", \"col_2\", \"col_3\"],\"sourceMetastoreUris\":\"thrift://host:9083\"}";
  private final static String DROP_PARTITION_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"DROP_PARTITION\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"partition\":[\"col_1\", \"col_2\", \"col_3\"],\"sourceMetastoreUris\":\"thrift://host:9083\"}";

  private final static String CREATE_TABLE_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"CREATE_TABLE\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"sourceMetastoreUris\":\"thrift://host:9083\"}";
  private final static String INSERT_EVENT = "{\"protocolVersion\": \"1.0\",\"eventType\": \"INSERT\",\"dbName\": \"some_db\",\"tableName\": \"some_table\",\"files\": [\"file:/a/b.txt\",\"file:/a/c.txt\"],\"fileChecksums\": [\"123\",\"456\"],\"partitionKeyValues\": {\"load_date\": \"2013-03-24\",\"variant_code\": \"EN\"},\"sourceMetastoreUris\": \"thrift://host:9083\"}";
  private final static String DROP_TABLE_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"DROP_TABLE\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"sourceMetastoreUris\":\"thrift://host:9083\"}";

  private final static String TEST_DB = "some_db";
  private final static String TEST_TABLE = "some_table";

  private Table table;
  private List<FieldSchema> partitionKeys;

  private final MessageDecoder decoder = new MessageDecoder() {
    @Override
    public byte[] decode(Message message) {
      return message.getBody().getBytes();
    }
  };

  public void init() {
    table = new Table();

    FieldSchema partitionColumn1 = new FieldSchema("Column_1", "String", "");
    FieldSchema partitionColumn2 = new FieldSchema("Column_2", "String", "");
    FieldSchema partitionColumn3 = new FieldSchema("Column_3", "String", "");
    partitionKeys = ImmutableList.of(partitionColumn1, partitionColumn2, partitionColumn3);

    table.setDbName(TEST_DB);
    table.setTableName(TEST_TABLE);
    table.setPartitionKeys(partitionKeys);
  }

  @Test
  public void addPartitionEvent() throws Exception {
    Message message = new Message().withBody(ADD_PARTITION_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryAddPartitionEvent addPartitionEvent = (SerializableApiaryAddPartitionEvent) processedEvent;

    assertThat(addPartitionEvent).isEqualTo(addPartitionEvent);
  }

  @Test
  public void alterPartitionEvent() throws Exception {
    Message message = new Message().withBody(ALTER_PARTITION_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryAlterPartitionEvent addPartitionEvent = (SerializableApiaryAlterPartitionEvent) processedEvent;

    assertThat(addPartitionEvent).isEqualTo(addPartitionEvent);
  }

  @Test
  public void dropPartitionEvent() throws Exception {
    Message message = new Message().withBody(DROP_PARTITION_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryDropPartitionEvent addPartitionEvent = (SerializableApiaryDropPartitionEvent) processedEvent;

    assertThat(addPartitionEvent).isEqualTo(addPartitionEvent);
  }

  @Test
  public void createTableEvent() throws Exception {
    Message message = new Message().withBody(CREATE_TABLE_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryCreateTableEvent addPartitionEvent = (SerializableApiaryCreateTableEvent) processedEvent;

    assertThat(addPartitionEvent).isEqualTo(addPartitionEvent);
  }

  @Test
  public void insertTableEvent() throws Exception {
    Message message = new Message().withBody(INSERT_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryInsertTableEvent addPartitionEvent = (SerializableApiaryInsertTableEvent) processedEvent;

    assertThat(addPartitionEvent).isEqualTo(addPartitionEvent);
  }

  @Test
  public void dropTableEvent() throws Exception {
    Message message = new Message().withBody(DROP_TABLE_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryDropTableEvent addPartitionEvent = (SerializableApiaryDropTableEvent) processedEvent;

    assertThat(addPartitionEvent).isEqualTo(addPartitionEvent);
  }

  // private void assertEvent(SerializableListenerEvent event, EventType eventType) {
  //
  // assertThat(event.getDatabaseName()).equals(TEST_DB);
  // assertThat(event.getTableName()).equals(TEST_DB);
  // assertThat(event.getSourceMetastoreUris()).equals(TEST_DB);
  // assertThat(event.get).equals(TEST_DB);
  // }

}
