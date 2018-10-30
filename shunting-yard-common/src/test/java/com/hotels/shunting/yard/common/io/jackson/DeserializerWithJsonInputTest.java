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
package com.hotels.shunting.yard.common.io.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.shunting.yard.common.event.AddPartitionEvent;
import com.hotels.shunting.yard.common.event.AlterPartitionEvent;
import com.hotels.shunting.yard.common.event.CreateTableEvent;
import com.hotels.shunting.yard.common.event.DropPartitionEvent;
import com.hotels.shunting.yard.common.event.DropTableEvent;
import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.event.InsertTableEvent;
import com.hotels.shunting.yard.common.event.ListenerEvent;

@RunWith(MockitoJUnitRunner.class)
public class DeserializerWithJsonInputTest {
  private final JsonMetaStoreEventDeserializer metaStoreEventDeserializer = new JsonMetaStoreEventDeserializer();
  private final ApiarySqsMessageDeserializer sqsDeserializer = new ApiarySqsMessageDeserializer(
      metaStoreEventDeserializer);

  private final static String BASE_EVENT_FROM_SNS = "{"
      + "  \"Type\" : \"Notification\","
      + "  \"MessageId\" : \"message-id\","
      + "  \"TopicArn\" : \"arn:aws:sns:us-west-2:sns-topic\","
      + "  \"Timestamp\" : \"2018-10-23T13:01:54.507Z\","
      + "  \"SignatureVersion\" : \"1\","
      + "  \"Signature\" : \"signature\","
      + "  \"SigningCertURL\" : \"https://sns.us-west-2.amazonaws.com/SimpleNotificationService-xxxx\","
      + "  \"UnsubscribeURL\" : \"https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:440407435941:sns-topic\",";

  private final static String ADD_PARTITION_EVENT = "\"Message\" : \"{\\\"protocolVersion\\\":\\\"1.0\\\",\\\"eventType\\\":\\\"ADD_PARTITION\\\",\\\"dbName\\\":\\\"some_db\\\",\\\"tableName\\\":\\\"some_table\\\",\\\"partitionKeys\\\":{\\\"col_1\\\": \\\"string\\\", \\\"col_2\\\": \\\"integer\\\",\\\"col_3\\\": \\\"string\\\"},\\\"partitionValues\\\":[\\\"val_1\\\", \\\"val_2\\\", \\\"val_3\\\"]}\"";
  private final static String ALTER_PARTITION_EVENT = "\"Message\" : \"{\\\"protocolVersion\\\":\\\"1.0\\\",\\\"eventType\\\":\\\"ALTER_PARTITION\\\",\\\"dbName\\\":\\\"some_db\\\",\\\"tableName\\\":\\\"some_table\\\",\\\"partitionKeys\\\": {\\\"col_1\\\": \\\"string\\\", \\\"col_2\\\": \\\"integer\\\",\\\"col_3\\\": \\\"string\\\"}, \\\"partitionValues\\\":[\\\"val_1\\\", \\\"val_2\\\", \\\"val_3\\\"],\\\"oldPartitionValues\\\": [\\\"val_4\\\", \\\"val_5\\\", \\\"val_6\\\"]}\"";
  private final static String DROP_PARTITION_EVENT = "\"Message\" : \"{\\\"protocolVersion\\\":\\\"1.0\\\",\\\"eventType\\\":\\\"DROP_PARTITION\\\",\\\"dbName\\\":\\\"some_db\\\",\\\"tableName\\\":\\\"some_table\\\",\\\"partitionKeys\\\": {\\\"col_1\\\": \\\"string\\\", \\\"col_2\\\": \\\"integer\\\",\\\"col_3\\\": \\\"string\\\"},\\\"partitionValues\\\":[\\\"val_1\\\", \\\"val_2\\\", \\\"val_3\\\"]}\"";

  private final static String CREATE_TABLE_EVENT = "\"Message\" : \"{\\\"protocolVersion\\\":\\\"1.0\\\",\\\"eventType\\\":\\\"CREATE_TABLE\\\",\\\"dbName\\\":\\\"some_db\\\",\\\"tableName\\\":\\\"some_table\\\"}\"";
  private final static String DROP_TABLE_EVENT = "\"Message\" : \"{\\\"protocolVersion\\\":\\\"1.0\\\",\\\"eventType\\\":\\\"DROP_TABLE\\\",\\\"dbName\\\":\\\"some_db\\\",\\\"tableName\\\":\\\"some_table\\\"}\"";
  private final static String INSERT_EVENT = "\"Message\" : \"{\\\"protocolVersion\\\": \\\"1.0\\\",\\\"eventType\\\": \\\"INSERT\\\",\\\"dbName\\\": \\\"some_db\\\",\\\"tableName\\\": \\\"some_table\\\",\\\"files\\\": [\\\"file:/a/b.txt\\\",\\\"file:/a/c.txt\\\"],\\\"fileChecksums\\\": [\\\"123\\\",\\\"456\\\"],\\\"partitionKeyValues\\\": {\\\"col_1\\\": \\\"val_1\\\",\\\"col_2\\\": \\\"val_2\\\", \\\"col_3\\\": \\\"val_3\\\"}}\"";

  private static final Map<String, String> PARTITION_KEYS_MAP = ImmutableMap
      .of("col_1", "string", "col_2", "integer", "col_3", "string");
  private static final List<String> PARTITION_VALUES = ImmutableList.of("val_1", "val_2", "val_3");
  private static final List<String> OLD_PARTITION_VALUES = ImmutableList.of("val_4", "val_5", "val_6");
  private final static String TEST_DB = "some_db";
  private final static String TEST_TABLE = "some_table";

  @Test
  public void addPartitionEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(ADD_PARTITION_EVENT));
    AddPartitionEvent addPartitionEvent = (AddPartitionEvent) processedEvent;

    assertThat(addPartitionEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(addPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(addPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(addPartitionEvent.getEventType()).isEqualTo(EventType.ADD_PARTITION);
    assertThat(addPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(addPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
  }

  @Test
  public void alterPartitionEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(ALTER_PARTITION_EVENT));
    AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) processedEvent;

    assertThat(alterPartitionEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(alterPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(alterPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(alterPartitionEvent.getEventType()).isEqualTo(EventType.ALTER_PARTITION);
    assertThat(alterPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(alterPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);

    assertThat(alterPartitionEvent.getOldPartitionValues()).isEqualTo(OLD_PARTITION_VALUES);
  }

  @Test
  public void dropPartitionEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(DROP_PARTITION_EVENT));
    DropPartitionEvent dropPartitionEvent = (DropPartitionEvent) processedEvent;

    assertThat(dropPartitionEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(dropPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(dropPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(dropPartitionEvent.getEventType()).isEqualTo(EventType.DROP_PARTITION);
    assertThat(dropPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(dropPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
  }

  @Test
  public void createTableEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(CREATE_TABLE_EVENT));
    CreateTableEvent createTableEvent = (CreateTableEvent) processedEvent;

    assertThat(createTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(createTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(createTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(createTableEvent.getEventType()).isEqualTo(EventType.CREATE_TABLE);
  }

  @Test
  public void insertTableEvent() throws Exception {
    List<String> expectedFiles = ImmutableList.of("file:/a/b.txt", "file:/a/c.txt");
    List<String> expectedFileChecksums = ImmutableList.of("123", "456");
    Map<String, String> PARTITION_KEY_VALUE_MAP = ImmutableMap.of("col_1", "val_1", "col_2", "val_2", "col_3", "val_3");

    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(INSERT_EVENT));
    InsertTableEvent insertTableEvent = (InsertTableEvent) processedEvent;

    assertThat(insertTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(insertTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(insertTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(insertTableEvent.getEventType()).isEqualTo(EventType.INSERT);
    assertThat(insertTableEvent.getPartitionKeyValues()).isEqualTo(PARTITION_KEY_VALUE_MAP);

    assertThat(insertTableEvent.getFiles()).isEqualTo(expectedFiles);
    assertThat(insertTableEvent.getFileChecksums()).isEqualTo(expectedFileChecksums);
  }

  @Test
  public void dropTableEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(DROP_TABLE_EVENT));
    DropTableEvent dropTableEvent = (DropTableEvent) processedEvent;

    assertThat(dropTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(dropTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(dropTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(dropTableEvent.getEventType()).isEqualTo(EventType.DROP_TABLE);
  }

  private String getSnsMessage(String eventMessage) {
    return BASE_EVENT_FROM_SNS + eventMessage + "}";
  }

}
