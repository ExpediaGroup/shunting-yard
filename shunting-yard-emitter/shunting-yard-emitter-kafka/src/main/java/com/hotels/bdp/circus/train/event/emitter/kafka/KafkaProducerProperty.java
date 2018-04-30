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
package com.hotels.bdp.circus.train.event.emitter.kafka;

public enum KafkaProducerProperty {
  TOPIC("topic", null),
  BOOTSTRAP_SERVERS("bootstrap.servers", null),
  CLIENT_ID("client.id", "CircusTrainEventDrivenEmitter"),
  ACKS("acks", "all"),
  RETRIES("retries", 3),
  MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION("max.in.flight.requests.per.connection", 1),
  BATCH_SIZE("batch.size", 16384),
  LINGER_MS("linger.ms", 1L),
  BUFFER_MEMORY("buffer.memory", 33554432L);

  private static final String PROPERTY_PREFIX = "com.hotels.bdp.circus.train.event.emitter.kafka.";

  private final String unPrefixedKey;
  private final Object defaultValue;

  private KafkaProducerProperty(String unPrefixedKey, Object defaultValue) {
    this.unPrefixedKey = unPrefixedKey;
    this.defaultValue = defaultValue;
  }

  public String key() {
    return new StringBuffer(PROPERTY_PREFIX).append(unPrefixedKey).toString();
  }

  public String unPrefixedKey() {
    return unPrefixedKey;
  }

  public Object defaultValue() {
    return defaultValue;
  }

  @Override
  public String toString() {
    return key();
  }

}
