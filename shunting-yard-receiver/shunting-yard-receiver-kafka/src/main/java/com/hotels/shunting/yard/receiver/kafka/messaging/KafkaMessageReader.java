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
package com.hotels.shunting.yard.receiver.kafka.messaging;

import static com.hotels.shunting.yard.common.Preconditions.checkNotNull;
import static com.hotels.shunting.yard.common.PropertyUtils.stringProperty;
import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.TOPIC;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.SerDeException;
import com.hotels.shunting.yard.common.messaging.MessageReader;

public class KafkaMessageReader implements MessageReader {

  private String topic;
  private final Configuration conf;
  private final KafkaConsumer<Long, byte[]> consumer;
  private final MetaStoreEventSerDe eventSerDe;
  private Iterator<ConsumerRecord<Long, byte[]>> records;

  public KafkaMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe) {
    this(conf, eventSerDe, new HiveMetaStoreEventKafkaConsumer(conf));
  }

  @VisibleForTesting
  KafkaMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe, KafkaConsumer<Long, byte[]> consumer) {
    this.conf = conf;
    this.consumer = consumer;
    this.eventSerDe = eventSerDe;
    init();
  }

  private void init() {
    topic = checkNotNull(stringProperty(conf, TOPIC), "Property " + TOPIC + " is not set");
    consumer.subscribe(Arrays.asList(topic));
  }

  @Override
  public void close() throws IOException {
    consumer.close();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Cannot remove message from Kafka topic");
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public SerializableListenerEvent next() {
    readRecordsIfNeeded();
    return eventPayLoad(records.next());
  }

  private void readRecordsIfNeeded() {
    while (records == null || !records.hasNext()) {
      records = consumer.poll(Long.MAX_VALUE).iterator();
    }
  }

  private SerializableListenerEvent eventPayLoad(ConsumerRecord<Long, byte[]> message) {
    try {
      return eventSerDe.unmarshall(message.value());
    } catch (Exception e) {
      // TODO this may be removed when we get rid off checked exceptions in the SerDe contract
      throw new SerDeException("Unable to unmarshall event", e);
    }
  }

}
