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

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.assertj.core.api.Assertions.assertThat;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.hotels.shunting.yard.common.event.AddPartitionEvent;
import com.hotels.shunting.yard.common.event.AlterPartitionEvent;
import com.hotels.shunting.yard.common.event.AlterTableEvent;
import com.hotels.shunting.yard.common.event.CreateTableEvent;
import com.hotels.shunting.yard.common.event.DropPartitionEvent;
import com.hotels.shunting.yard.common.event.DropTableEvent;
import com.hotels.shunting.yard.common.event.EventType;
import com.hotels.shunting.yard.common.event.InsertTableEvent;
import com.hotels.shunting.yard.common.event.ListenerEvent;

@RunWith(MockitoJUnitRunner.class)
public class DeserializerWithJsonInputTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    OBJECT_MAPPER.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Rule
  public DataFolder dataFolder = new ClassDataFolder();

  private final JsonMetaStoreEventDeserializer metaStoreEventDeserializer = new JsonMetaStoreEventDeserializer(
      OBJECT_MAPPER);
  private final ApiarySqsMessageDeserializer sqsDeserializer = new ApiarySqsMessageDeserializer(
      metaStoreEventDeserializer, OBJECT_MAPPER);

  private final static String BASE_EVENT_FROM_SNS = "{"
      + "  \"Type\" : \"Notification\","
      + "  \"MessageId\" : \"message-id\","
      + "  \"TopicArn\" : \"arn:aws:sns:us-west-2:sns-topic\","
      + "  \"Timestamp\" : \"2018-10-23T13:01:54.507Z\","
      + "  \"SignatureVersion\" : \"1\","
      + "  \"Signature\" : \"signature\","
      + "  \"SigningCertURL\" : \"https://sns.us-west-2.amazonaws.com/SimpleNotificationService-xxxx\","
      + "  \"UnsubscribeURL\" : \"https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:440407435941:sns-topic\",";

  private String addPartitionEvent;
  private String alterPartitionEvent;
  private String createTableEvent;
  private String dropPartitionEvent;
  private String insertEvent;
  private String alterTableEvent;
  private String dropTableEvent;

  private static final Map<String, String> PARTITION_KEYS_MAP = ImmutableMap
      .of("col_1", "string", "col_2", "integer", "col_3", "string");
  private static final List<String> PARTITION_VALUES = ImmutableList.of("val_1", "val_2", "val_3");
  private static final List<String> OLD_PARTITION_VALUES = ImmutableList.of("val_4", "val_5", "val_6");

  private static final Map<String, String> TABLE_PARAM_MAP = ImmutableMap.of("param_1", "val_1", "param_2", "val_2");

  private final static String TEST_DB = "some_db";
  private final static String TEST_TABLE = "some_table";
  private final static String TEST_TABLE_LOCATION = "s3://table_location";
  private final static String OLD_TEST_TABLE_LOCATION = "s3://old_table_location";
  private final static String PARTITION_LOCATION = "s3://table_location/partition_location";
  private final static String OLD_PARTITION_LOCATION = "s3://table_location/old_partition_location";

  @Before
  public void init() throws IOException {
    addPartitionEvent = new String(Files.readAllBytes(dataFolder.getFile("add_partition.json").toPath()), UTF_8);
    alterPartitionEvent = new String(Files.readAllBytes(dataFolder.getFile("alter_partition.json").toPath()), UTF_8);
    dropPartitionEvent = new String(Files.readAllBytes(dataFolder.getFile("drop_partition.json").toPath()), UTF_8);
    createTableEvent = new String(Files.readAllBytes(dataFolder.getFile("create_table.json").toPath()), UTF_8);
    insertEvent = new String(Files.readAllBytes(dataFolder.getFile("insert_table.json").toPath()), UTF_8);
    alterTableEvent = new String(Files.readAllBytes(dataFolder.getFile("alter_table.json").toPath()), UTF_8);
    dropTableEvent = new String(Files.readAllBytes(dataFolder.getFile("drop_table.json").toPath()), UTF_8);
  }

  @Test
  public void addPartitionEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(addPartitionEvent));
    AddPartitionEvent addPartitionEvent = (AddPartitionEvent) processedEvent;

    assertThat(addPartitionEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(addPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(addPartitionEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(addPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(addPartitionEvent.getEventType()).isEqualTo(EventType.ADD_PARTITION);
    assertThat(addPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(addPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
    assertThat(addPartitionEvent.getPartitionLocation()).isEqualTo(PARTITION_LOCATION);
    assertThat(addPartitionEvent.getTableParameters()).isEqualTo(TABLE_PARAM_MAP);
  }

  @Test
  public void alterPartitionEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(alterPartitionEvent));
    AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) processedEvent;

    assertThat(alterPartitionEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(alterPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(alterPartitionEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(alterPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(alterPartitionEvent.getEventType()).isEqualTo(EventType.ALTER_PARTITION);
    assertThat(alterPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(alterPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
    assertThat(alterPartitionEvent.getPartitionLocation()).isEqualTo(PARTITION_LOCATION);
    assertThat(alterPartitionEvent.getOldPartitionValues()).isEqualTo(OLD_PARTITION_VALUES);
    assertThat(alterPartitionEvent.getOldPartitionLocation()).isEqualTo(OLD_PARTITION_LOCATION);
    assertThat(alterPartitionEvent.getTableParameters()).isEqualTo(TABLE_PARAM_MAP);
  }

  @Test
  public void dropPartitionEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(dropPartitionEvent));
    DropPartitionEvent dropPartitionEvent = (DropPartitionEvent) processedEvent;

    assertThat(dropPartitionEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(dropPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(dropPartitionEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(dropPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(dropPartitionEvent.getEventType()).isEqualTo(EventType.DROP_PARTITION);
    assertThat(dropPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(dropPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
    assertThat(dropPartitionEvent.getPartitionLocation()).isEqualTo(PARTITION_LOCATION);
    assertThat(dropPartitionEvent.getTableParameters()).isEqualTo(TABLE_PARAM_MAP);
  }

  @Test
  public void createTableEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(createTableEvent));
    CreateTableEvent createTableEvent = (CreateTableEvent) processedEvent;

    assertThat(createTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(createTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(createTableEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(createTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(createTableEvent.getEventType()).isEqualTo(EventType.CREATE_TABLE);
    assertThat(createTableEvent.getTableParameters()).isEqualTo(Maps.newHashMap());
  }

  @Test
  public void insertTableEvent() throws Exception {
    List<String> expectedFiles = ImmutableList.of("file:/a/b.txt", "file:/a/c.txt");
    List<String> expectedFileChecksums = ImmutableList.of("123", "456");
    Map<String, String> PARTITION_KEY_VALUE_MAP = ImmutableMap.of("col_1", "val_1", "col_2", "val_2", "col_3", "val_3");

    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(insertEvent));
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
  public void alterTableEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(alterTableEvent));
    AlterTableEvent alterTableEvent = (AlterTableEvent) processedEvent;

    assertThat(alterTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(alterTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(alterTableEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(alterTableEvent.getOldTableLocation()).isEqualTo(OLD_TEST_TABLE_LOCATION);
    assertThat(alterTableEvent.getOldTableName()).isEqualTo(TEST_TABLE);
    assertThat(alterTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(alterTableEvent.getEventType()).isEqualTo(EventType.ALTER_TABLE);
    assertThat(alterTableEvent.getTableParameters()).isEqualTo(TABLE_PARAM_MAP);
  }

  @Test
  public void dropTableEvent() throws Exception {
    ListenerEvent processedEvent = sqsDeserializer.unmarshal(getSnsMessage(dropTableEvent));
    DropTableEvent dropTableEvent = (DropTableEvent) processedEvent;

    assertThat(dropTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(dropTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(dropTableEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(dropTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(dropTableEvent.getEventType()).isEqualTo(EventType.DROP_TABLE);
    assertThat(dropTableEvent.getTableParameters()).isEqualTo(Maps.newHashMap());
  }

  private String getSnsMessage(String eventMessage) {
    return BASE_EVENT_FROM_SNS + eventMessage + "}";
  }

}
