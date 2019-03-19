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
package com.hotels.shunting.yard.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import org.junit.Test;

public class MessageTest {

  private Message.Builder createMessageBuilder(String db, String table, String payload, long timestamp) {
    Message.Builder objectUnderTest = Message.builder();
    objectUnderTest.database(db);
    objectUnderTest.table(table);
    objectUnderTest.payload(payload);
    objectUnderTest.timestamp(timestamp);
    return objectUnderTest;
  }

  @Test
  public void nullPayload() {
    Message.Builder objectUnderTest = createMessageBuilder("test_db", "test_table",
        null, 1515585600000L);

    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> objectUnderTest.build())
        .withMessage("Parameter 'payload' is required");
  }

  @Test
  public void nullTable() {
    Message.Builder objectUnderTest = createMessageBuilder("test_db_1", "",
        "foo", 1553013512L);

    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> objectUnderTest.build())
        .withMessage("Parameter 'table' is required");
  }

  @Test
  public void typical() {
    Message.Builder objectUnderTest = createMessageBuilder("test_db_2", "test_table",
        "foo", 1269015963L);
    final Message message = objectUnderTest.build();

    assertThat(message).isNotNull();
    assertThat(message.getDatabase()).isEqualTo("test_db_2");
    assertThat(message.getPayload()).isEqualTo("foo");
    assertThat(message.getQualifiedTableName()).isEqualTo("test_db_2.test_table");
    assertThat(message.getTable()).isEqualTo("test_table");
    assertThat(message.getTimestamp()).isEqualTo(1269015963L);
  }

}
