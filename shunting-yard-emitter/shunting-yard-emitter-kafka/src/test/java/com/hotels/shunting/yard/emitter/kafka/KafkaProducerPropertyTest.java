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
package com.hotels.shunting.yard.emitter.kafka;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.ACKS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BATCH_SIZE;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BUFFER_MEMORY;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.CLIENT_ID;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.LINGER_MS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.RETRIES;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.TOPIC;

import org.junit.Test;

import com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty;

public class KafkaProducerPropertyTest {

  private static <T> Object asObject(T t) {
    return t;
  }

  private static String prefixedKey(String key) {
    return "com.hotels.bdp.circus.train.event.emitter.kafka." + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(KafkaProducerProperty.values().length, is(9));
  }

  @Test
  public void topic() {
    assertThat(TOPIC.unPrefixedKey(), is("topic"));
    assertThat(TOPIC.key(), is(prefixedKey("topic")));
    assertThat(TOPIC.defaultValue(), is(nullValue()));
  }

  @Test
  public void bootstrapServers() {
    assertThat(BOOTSTRAP_SERVERS.unPrefixedKey(), is("bootstrap.servers"));
    assertThat(BOOTSTRAP_SERVERS.key(), is(prefixedKey("bootstrap.servers")));
    assertThat(BOOTSTRAP_SERVERS.defaultValue(), is(nullValue()));
  }

  @Test
  public void clientId() {
    assertThat(CLIENT_ID.unPrefixedKey(), is("client.id"));
    assertThat(CLIENT_ID.key(), is(prefixedKey("client.id")));
    assertThat(CLIENT_ID.defaultValue(), is(asObject("CircusTrainEventDrivenEmitter")));
  }

  @Test
  public void acks() {
    assertThat(ACKS.unPrefixedKey(), is("acks"));
    assertThat(ACKS.key(), is(prefixedKey("acks")));
    assertThat(ACKS.defaultValue(), is(asObject("all")));
  }

  @Test
  public void retires() {
    assertThat(RETRIES.unPrefixedKey(), is("retries"));
    assertThat(RETRIES.key(), is(prefixedKey("retries")));
    assertThat(RETRIES.defaultValue(), is(asObject(3)));
  }

  @Test
  public void maxInFlightRequestsPerConnection() {
    assertThat(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.unPrefixedKey(), is("max.in.flight.requests.per.connection"));
    assertThat(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.key(), is(prefixedKey("max.in.flight.requests.per.connection")));
    assertThat(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.defaultValue(), is(asObject(1)));
  }

  @Test
  public void batchSize() {
    assertThat(BATCH_SIZE.unPrefixedKey(), is("batch.size"));
    assertThat(BATCH_SIZE.key(), is(prefixedKey("batch.size")));
    assertThat(BATCH_SIZE.defaultValue(), is(asObject(16384)));
  }

  @Test
  public void lingerMs() {
    assertThat(LINGER_MS.unPrefixedKey(), is("linger.ms"));
    assertThat(LINGER_MS.key(), is(prefixedKey("linger.ms")));
    assertThat(LINGER_MS.defaultValue(), is(asObject(1L)));
  }

  @Test
  public void bufferMemory() {
    assertThat(BUFFER_MEMORY.unPrefixedKey(), is("buffer.memory"));
    assertThat(BUFFER_MEMORY.key(), is(prefixedKey("buffer.memory")));
    assertThat(BUFFER_MEMORY.defaultValue(), is(asObject(33554432L)));
  }

}
