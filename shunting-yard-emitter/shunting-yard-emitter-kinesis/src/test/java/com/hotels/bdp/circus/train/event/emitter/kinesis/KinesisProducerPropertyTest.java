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
package com.hotels.bdp.circus.train.event.emitter.kinesis;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.MAX_CONNECTIONS;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.RECORD_MAX_BUFFERED_TIME;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.REGION;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.REQUEST_TIMEOUT;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.RETRIES;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.STREAM;

import org.junit.Test;

public class KinesisProducerPropertyTest {

  private static <T> Object asObject(T t) {
    return t;
  }

  private static String prefixedKey(String key) {
    return "com.hotels.bdp.circus.train.event.emitter.kinesis." + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(KinesisProducerProperty.values().length, is(6));
  }

  @Test
  public void stream() {
    assertThat(STREAM.unPrefixedKey(), is("stream"));
    assertThat(STREAM.key(), is(prefixedKey("stream")));
    assertThat(STREAM.defaultValue(), is(nullValue()));
  }

  @Test
  public void region() {
    assertThat(REGION.unPrefixedKey(), is("region"));
    assertThat(REGION.key(), is(prefixedKey("region")));
    assertThat(REGION.defaultValue(), is(asObject("us-west-2")));
  }

  @Test
  public void maxConnections() {
    assertThat(MAX_CONNECTIONS.unPrefixedKey(), is("max.connections"));
    assertThat(MAX_CONNECTIONS.key(), is(prefixedKey("max.connections")));
    assertThat(MAX_CONNECTIONS.defaultValue(), is(asObject(1L)));
  }

  @Test
  public void requestTimeout() {
    assertThat(REQUEST_TIMEOUT.unPrefixedKey(), is("request.timeout"));
    assertThat(REQUEST_TIMEOUT.key(), is(prefixedKey("request.timeout")));
    assertThat(REQUEST_TIMEOUT.defaultValue(), is(asObject(60000L)));
  }

  @Test
  public void maxInFlightRequestsPerConnection() {
    assertThat(RECORD_MAX_BUFFERED_TIME.unPrefixedKey(), is("record.max.buffered.time"));
    assertThat(RECORD_MAX_BUFFERED_TIME.key(), is(prefixedKey("record.max.buffered.time")));
    assertThat(RECORD_MAX_BUFFERED_TIME.defaultValue(), is(asObject(15000L)));
  }

  @Test
  public void retires() {
    assertThat(RETRIES.unPrefixedKey(), is("retries"));
    assertThat(RETRIES.key(), is(prefixedKey("retries")));
    assertThat(RETRIES.defaultValue(), is(asObject(3)));
  }

}
