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
package com.hotels.shunting.yard.emitter.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.MAX_CONNECTIONS;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.RECORD_MAX_BUFFERED_TIME;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.REGION;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.REQUEST_TIMEOUT;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.RETRIES;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.STREAM;

import org.junit.Test;

public class KinesisProducerPropertyTest {

  private static String prefixedKey(String key) {
    return "com.hotels.shunting.yard.event.emitter.kinesis." + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(KinesisProducerProperty.values().length).isEqualTo(6);
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
  public void maxConnections() {
    assertThat(MAX_CONNECTIONS.unPrefixedKey()).isEqualTo("max.connections");
    assertThat(MAX_CONNECTIONS.key()).isEqualTo(prefixedKey("max.connections"));
    assertThat(MAX_CONNECTIONS.defaultValue()).isEqualTo(1L);
  }

  @Test
  public void requestTimeout() {
    assertThat(REQUEST_TIMEOUT.unPrefixedKey()).isEqualTo("request.timeout");
    assertThat(REQUEST_TIMEOUT.key()).isEqualTo(prefixedKey("request.timeout"));
    assertThat(REQUEST_TIMEOUT.defaultValue()).isEqualTo(60000L);
  }

  @Test
  public void maxInFlightRequestsPerConnection() {
    assertThat(RECORD_MAX_BUFFERED_TIME.unPrefixedKey()).isEqualTo("record.max.buffered.time");
    assertThat(RECORD_MAX_BUFFERED_TIME.key()).isEqualTo(prefixedKey("record.max.buffered.time"));
    assertThat(RECORD_MAX_BUFFERED_TIME.defaultValue()).isEqualTo(15000L);
  }

  @Test
  public void retires() {
    assertThat(RETRIES.unPrefixedKey()).isEqualTo("retries");
    assertThat(RETRIES.key()).isEqualTo(prefixedKey("retries"));
    assertThat(RETRIES.defaultValue()).isEqualTo(3);
  }

}
