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
package com.hotels.shunting.yard.receiver.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.APPLICTION_ID;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.BUFFER_CAPACITY;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.MAX_RECORDS;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.REGION;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.STREAM;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.WORKER_ID;

import org.junit.Test;

public class KinesisConsumerPropertyTest {

  private static String prefixedKey(String key) {
    return "com.hotels.shunting.yard.event.receiver.kinesis." + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(KinesisConsumerProperty.values().length).isEqualTo(6);
  }

  @Test
  public void stream() {
    assertThat(STREAM.unPrefixedKey()).isEqualTo("stream");
    assertThat(STREAM.key()).isEqualTo(prefixedKey("stream"));
    assertThat(STREAM.defaultValue()).isNull();
  }

  @Test
  public void region() {
    assertThat(REGION.unPrefixedKey()).isEqualTo("region");
    assertThat(REGION.key()).isEqualTo(prefixedKey("region"));
    assertThat(REGION.defaultValue()).isEqualTo("us-west-2");
  }

  @Test
  public void applicationId() {
    assertThat(APPLICTION_ID.unPrefixedKey()).isEqualTo("application.id");
    assertThat(APPLICTION_ID.key()).isEqualTo(prefixedKey("application.id"));
    assertThat(APPLICTION_ID.defaultValue()).isNull();
  }

  @Test
  public void workerId() {
    assertThat(WORKER_ID.unPrefixedKey()).isEqualTo("worker.id");
    assertThat(WORKER_ID.key()).isEqualTo(prefixedKey("worker.id"));
    assertThat(WORKER_ID.defaultValue()).isEqualTo("CircusTrainEventDrivenReceiver");
  }

  @Test
  public void maxRecords() {
    assertThat(MAX_RECORDS.unPrefixedKey()).isEqualTo("max.records");
    assertThat(MAX_RECORDS.key()).isEqualTo(prefixedKey("max.records"));
    assertThat(MAX_RECORDS.defaultValue()).isEqualTo(30);
  }

  @Test
  public void bufferCapacity() {
    assertThat(BUFFER_CAPACITY.unPrefixedKey()).isEqualTo("buffer.capacity");
    assertThat(BUFFER_CAPACITY.key()).isEqualTo(prefixedKey("buffer.capacity"));
    assertThat(BUFFER_CAPACITY.defaultValue()).isEqualTo(100);
  }

}
