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
package com.hotels.bdp.circus.train.event.emitter.kafka.messaging;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import static com.hotels.bdp.circus.train.event.emitter.kafka.KafkaProducerProperty.ACKS;
import static com.hotels.bdp.circus.train.event.emitter.kafka.KafkaProducerProperty.BATCH_SIZE;
import static com.hotels.bdp.circus.train.event.emitter.kafka.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.hotels.bdp.circus.train.event.emitter.kafka.KafkaProducerProperty.BUFFER_MEMORY;
import static com.hotels.bdp.circus.train.event.emitter.kafka.KafkaProducerProperty.LINGER_MS;
import static com.hotels.bdp.circus.train.event.emitter.kafka.KafkaProducerProperty.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static com.hotels.bdp.circus.train.event.emitter.kafka.KafkaProducerProperty.RETRIES;
import static com.hotels.bdp.circus.train.event.emitter.kafka.KafkaProducerProperty.TOPIC;
import static com.hotels.bdp.circus.train.event.emitter.kafka.messaging.KafkaMessageTaskFactory.kafkaProperties;
import static com.hotels.bdp.circus.train.event.emitter.kafka.messaging.KafkaMessageTaskFactory.topic;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.circus.train.event.common.messaging.Message;
import com.hotels.bdp.circus.train.event.common.messaging.MessageTask;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMessageTaskFactoryTest {

  private static final String TOPIC_NAME = "topic";

  private static <T> Object asObject(T t) {
    return t;
  }

  private @Mock Message message;

  private final Configuration conf = new Configuration();

  @Test
  public void taskType() {
    @SuppressWarnings("unchecked")
    KafkaProducer<Long, byte[]> producer = mock(KafkaProducer.class);
    MessageTask task = new KafkaMessageTaskFactory(TOPIC_NAME, producer).newTask(message);
    assertThat(task, is(instanceOf(KafkaMessageTask.class)));
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
    assertThat(props.get("bootstrap.servers"), is(asObject("broker")));
    assertThat(props.get("acks"), is(asObject("acknowledgements")));
    assertThat(props.get("retries"), is(asObject(1)));
    assertThat(props.get("max.in.flight.requests.per.connection"), is(asObject(2)));
    assertThat(props.get("batch.size"), is(asObject(3)));
    assertThat(props.get("linger.ms"), is(asObject(4L)));
    assertThat(props.get("buffer.memory"), is(asObject(5L)));
    assertThat(props.get("key.serializer"), is(asObject(LongSerializer.class.getName())));
    assertThat(props.get("value.serializer"), is(asObject(ByteArraySerializer.class.getName())));
  }

  @Test
  public void topicIsNotNull() {
    conf.set(TOPIC.key(), TOPIC_NAME);
    assertThat(topic(conf), is(TOPIC_NAME));
  }

  @Test(expected = NullPointerException.class)
  public void topicIsNull() {
    topic(conf);
  }

}
