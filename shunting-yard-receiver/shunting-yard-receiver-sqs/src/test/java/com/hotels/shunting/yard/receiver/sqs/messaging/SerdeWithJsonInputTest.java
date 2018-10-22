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
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.shunting.yard.common.event.EventType;
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
  private final static String ADD_PARTITION_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"ADD_PARTITION\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"partitionKeys\":{\"col_1\": \"string\", \"col_2\": \"integer\",\"col_3\": \"string\"}, \"partitionValues\":[\"val_1\", \"val_2\", \"val_3\"]}";
  private final static String ALTER_PARTITION_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"ALTER_PARTITION\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"partitionKeys\": {\"col_1\": \"string\", \"col_2\": \"integer\",\"col_3\": \"string\"}, \"partitionValues\":[\"val_1\", \"val_2\", \"val_3\"],\"oldPartitionValues\": [\"val_4\", \"val_5\", \"val_6\"]}";
  private final static String DROP_PARTITION_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"DROP_PARTITION\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"partitionKeys\": {\"col_1\": \"string\", \"col_2\": \"integer\",\"col_3\": \"string\"},\"partitionValues\":[\"val_1\", \"val_2\", \"val_3\"]}";

  private final static String CREATE_TABLE_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"CREATE_TABLE\",\"dbName\":\"some_db\",\"tableName\":\"some_table\"}";
  private final static String INSERT_EVENT = "{\"protocolVersion\": \"1.0\",\"eventType\": \"INSERT\",\"dbName\": \"some_db\",\"tableName\": \"some_table\",\"files\": [\"file:/a/b.txt\",\"file:/a/c.txt\"],\"fileChecksums\": [\"123\",\"456\"],\"partitionKeyValues\": {\"col_1\": \"val_1\",\"col_2\": \"val_2\", \"col_3\": \"val_3\"}}";
  private final static String DROP_TABLE_EVENT = "{\"protocolVersion\":\"1.0\",\"eventType\":\"DROP_TABLE\",\"dbName\":\"some_db\",\"tableName\":\"some_table\"}";

  private static final Map<String, String> PARTITION_KEYS_MAP = ImmutableMap
      .of("col_1", "string", "col_2", "integer", "col_3", "string");
  private static final List<String> PARTITION_VALUES = ImmutableList.of("val_1", "val_2", "val_3");
  private static final List<String> OLD_PARTITION_VALUES = ImmutableList.of("val_4", "val_5", "val_6");
  private final static String TEST_DB = "some_db";
  private final static String TEST_TABLE = "some_table";

  private Table table;
  private List<FieldSchema> partitionKeys;

  private final MessageDecoder decoder = MessageDecoder.DEFAULT;

  public void init() {
    table = new Table();

    FieldSchema partitionColumn1 = new FieldSchema("col_1", "String", "");
    FieldSchema partitionColumn2 = new FieldSchema("col_2", "String", "");
    FieldSchema partitionColumn3 = new FieldSchema("col_3", "String", "");
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

    assertThat(addPartitionEvent.getDatabaseName()).isEqualTo(TEST_DB);
    assertThat(addPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(addPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(addPartitionEvent.getEventType()).isEqualTo(EventType.ADD_PARTITION);
    assertThat(addPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(addPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
  }

  @Test
  public void alterPartitionEvent() throws Exception {
    Message message = new Message().withBody(ALTER_PARTITION_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryAlterPartitionEvent alterPartitionEvent = (SerializableApiaryAlterPartitionEvent) processedEvent;

    assertThat(alterPartitionEvent.getDatabaseName()).isEqualTo(TEST_DB);
    assertThat(alterPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(alterPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(alterPartitionEvent.getEventType()).isEqualTo(EventType.ALTER_PARTITION);
    assertThat(alterPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(alterPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);

    assertThat(alterPartitionEvent.getOldPartitionValues()).isEqualTo(OLD_PARTITION_VALUES);
  }

  @Test
  public void dropPartitionEvent() throws Exception {
    Message message = new Message().withBody(DROP_PARTITION_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryDropPartitionEvent dropPartitionEvent = (SerializableApiaryDropPartitionEvent) processedEvent;

    assertThat(dropPartitionEvent.getDatabaseName()).isEqualTo(TEST_DB);
    assertThat(dropPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(dropPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(dropPartitionEvent.getEventType()).isEqualTo(EventType.DROP_PARTITION);
    assertThat(dropPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(dropPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
  }

  @Test
  public void createTableEvent() throws Exception {
    Message message = new Message().withBody(CREATE_TABLE_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryCreateTableEvent createTableEvent = (SerializableApiaryCreateTableEvent) processedEvent;

    assertThat(createTableEvent.getDatabaseName()).isEqualTo(TEST_DB);
    assertThat(createTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(createTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(createTableEvent.getEventType()).isEqualTo(EventType.CREATE_TABLE);
  }

  @Test
  public void insertTableEvent() throws Exception {
    List<String> expectedFiles = ImmutableList.of("file:/a/b.txt", "file:/a/c.txt");
    List<String> expectedFileChecksums = ImmutableList.of("123", "456");
    Map<String, String> PARTITION_KEY_VALUE_MAP = ImmutableMap.of("col_1", "val_1", "col_2", "val_2", "col_3", "val_3");

    Message message = new Message().withBody(INSERT_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryInsertTableEvent insertTableEvent = (SerializableApiaryInsertTableEvent) processedEvent;

    assertThat(insertTableEvent.getDatabaseName()).isEqualTo(TEST_DB);
    assertThat(insertTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(insertTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(insertTableEvent.getEventType()).isEqualTo(EventType.INSERT);
    assertThat(insertTableEvent.getPartitionKeyValues()).isEqualTo(PARTITION_KEY_VALUE_MAP);

    assertThat(insertTableEvent.getFiles()).isEqualTo(expectedFiles);
    assertThat(insertTableEvent.getFileChecksums()).isEqualTo(expectedFileChecksums);
  }

  @Test
  public void dropTableEvent() throws Exception {
    Message message = new Message().withBody(DROP_TABLE_EVENT);
    SerializableListenerEvent processedEvent = serDe.unmarshal(decoder.decode(message));
    SerializableApiaryDropTableEvent dropTableEvent = (SerializableApiaryDropTableEvent) processedEvent;

    assertThat(dropTableEvent.getDatabaseName()).isEqualTo(TEST_DB);
    assertThat(dropTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(dropTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(dropTableEvent.getEventType()).isEqualTo(EventType.DROP_TABLE);
  }

}
