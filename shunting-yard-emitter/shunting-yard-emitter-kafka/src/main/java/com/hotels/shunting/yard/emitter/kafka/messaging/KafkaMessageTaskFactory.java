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
package com.hotels.shunting.yard.emitter.kafka.messaging;

import static com.hotels.shunting.yard.common.Utils.checkNotNull;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.ACKS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BATCH_SIZE;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BUFFER_MEMORY;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.CLIENT_ID;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.LINGER_MS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.RETRIES;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.TOPIC;
import static com.hotels.shunting.yard.emitter.kafka.Utils.intProperty;
import static com.hotels.shunting.yard.emitter.kafka.Utils.longProperty;
import static com.hotels.shunting.yard.emitter.kafka.Utils.stringProperty;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.shunting.yard.common.messaging.Message;
import com.hotels.shunting.yard.common.messaging.MessageTask;
import com.hotels.shunting.yard.common.messaging.MessageTaskFactory;

/**
 * A {@link MessageTaskFactory} that create a task to post message to a Kafka topic. Note that in order to preserve the
 * order of the events the topic must have only single partition.
 */
public class KafkaMessageTaskFactory implements MessageTaskFactory {

  private final KafkaProducer<Long, byte[]> producer;
  private final String topic;
  private final int numberOfPartitions;

  public KafkaMessageTaskFactory(Configuration conf) {
    this(topic(conf), new KafkaProducer<Long, byte[]>(kafkaProperties(conf)));
  }

  @VisibleForTesting
  KafkaMessageTaskFactory(String topic, KafkaProducer<Long, byte[]> producer) {
    this.producer = producer;
    this.topic = topic;
    numberOfPartitions = producer.partitionsFor(topic).size();
  }

  @Override
  public MessageTask newTask(Message message) {
    return new KafkaMessageTask(producer, topic, numberOfPartitions, message);
  }

  @VisibleForTesting
  static Properties kafkaProperties(Configuration conf) {
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS.unPrefixedKey(),
        checkNotNull(stringProperty(conf, BOOTSTRAP_SERVERS), "Property " + BOOTSTRAP_SERVERS + " is not set"));
    props.put(CLIENT_ID.unPrefixedKey(),
        checkNotNull(stringProperty(conf, CLIENT_ID), "Property " + CLIENT_ID + " is not set"));
    props.put(ACKS.unPrefixedKey(), stringProperty(conf, ACKS));
    props.put(RETRIES.unPrefixedKey(), intProperty(conf, RETRIES));
    props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.unPrefixedKey(),
        intProperty(conf, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
    props.put(BATCH_SIZE.unPrefixedKey(), intProperty(conf, BATCH_SIZE));
    props.put(LINGER_MS.unPrefixedKey(), longProperty(conf, LINGER_MS));
    props.put(BUFFER_MEMORY.unPrefixedKey(), longProperty(conf, BUFFER_MEMORY));
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    return props;
  }

  @VisibleForTesting
  static String topic(Configuration conf) {
    return checkNotNull(stringProperty(conf, TOPIC), "Property " + TOPIC + " is not set");
  }

}
