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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.ACKS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BATCH_SIZE;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.BUFFER_MEMORY;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.LINGER_MS;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.RETRIES;
import static com.hotels.shunting.yard.emitter.kafka.KafkaProducerProperty.TOPIC;
import static com.hotels.shunting.yard.emitter.kafka.messaging.KafkaMessageTaskFactory.kafkaProperties;
import static com.hotels.shunting.yard.emitter.kafka.messaging.KafkaMessageTaskFactory.topic;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.shunting.yard.common.messaging.Message;
import com.hotels.shunting.yard.common.messaging.MessageTask;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMessageTaskFactoryTest {

  private static final String TOPIC_NAME = "topic";

  private @Mock Message message;

  private final Configuration conf = new Configuration();

  @Test
  public void taskType() {
    @SuppressWarnings("unchecked")
    KafkaProducer<Long, byte[]> producer = mock(KafkaProducer.class);
    MessageTask task = new KafkaMessageTaskFactory(TOPIC_NAME, producer).newTask(message);
    assertThat(task).isInstanceOf(KafkaMessageTask.class);
  }

  @Test
  public void populateKafkaProperties() {
    conf.set(BOOTSTRAP_SERVERS.key(), "broker");
    conf.set(ACKS.key(), "acknowledgements");
    conf.set(RETRIES.key(), "1");
    conf.set(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.key(), "2");
    conf.set(BATCH_SIZE.key(), "3");
    conf.set(LINGER_MS.key(), "4");
    conf.set(BUFFER_MEMORY.key(), "5");
    Properties props = kafkaProperties(conf);
    assertThat(props.get("bootstrap.servers")).isEqualTo("broker");
    assertThat(props.get("acks")).isEqualTo("acknowledgements");
    assertThat(props.get("retries")).isEqualTo(1);
    assertThat(props.get("max.in.flight.requests.per.connection")).isEqualTo(2);
    assertThat(props.get("batch.size")).isEqualTo(3);
    assertThat(props.get("linger.ms")).isEqualTo(4L);
    assertThat(props.get("buffer.memory")).isEqualTo(5L);
    assertThat(props.get("key.serializer")).isEqualTo(LongSerializer.class.getName());
    assertThat(props.get("value.serializer")).isEqualTo(ByteArraySerializer.class.getName());
  }

  @Test
  public void topicIsNotNull() {
    conf.set(TOPIC.key(), TOPIC_NAME);
    assertThat(topic(conf)).isEqualTo(TOPIC_NAME);
  }

  @Test(expected = NullPointerException.class)
  public void topicIsNull() {
    topic(conf);
  }

}
