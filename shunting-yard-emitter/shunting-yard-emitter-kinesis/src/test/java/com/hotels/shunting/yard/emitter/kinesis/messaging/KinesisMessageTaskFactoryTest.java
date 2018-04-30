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
package com.hotels.bdp.circus.train.event.emitter.kinesis.messaging;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.MAX_CONNECTIONS;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.RECORD_MAX_BUFFERED_TIME;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.REGION;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.REQUEST_TIMEOUT;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.RETRIES;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.KinesisProducerProperty.STREAM;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.messaging.KinesisMessageTaskFactory.kinesisProperties;
import static com.hotels.bdp.circus.train.event.emitter.kinesis.messaging.KinesisMessageTaskFactory.topic;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import com.hotels.bdp.circus.train.event.common.messaging.Message;
import com.hotels.bdp.circus.train.event.common.messaging.MessageTask;

public class KinesisMessageTaskFactoryTest {

  private static final String STREAM_NAME = "stream";

  private final Configuration conf = new Configuration();

  @Test
  public void taskType() {
    KinesisProducer producer = mock(KinesisProducer.class);
    Message message = mock(Message.class);
    MessageTask task = new KinesisMessageTaskFactory(STREAM_NAME, producer, 2).newTask(message);
    assertThat(task, is(instanceOf(KinesisMessageTask.class)));
  }

  @Test
  public void populateKinesisProperties() {
    conf.set(REGION.key(), "us-east-1");
    conf.set(MAX_CONNECTIONS.key(), "3");
    conf.set(REQUEST_TIMEOUT.key(), "1500");
    conf.set(RECORD_MAX_BUFFERED_TIME.key(), "250");
    conf.set(RETRIES.key(), "1");
    KinesisProducerConfiguration config = kinesisProperties(conf);
    assertThat(config.getRegion(), is("us-east-1"));
    assertThat(config.getMaxConnections(), is(3L));
    assertThat(config.getRequestTimeout(), is(1500L));
    assertThat(config.getRecordMaxBufferedTime(), is(250L));
  }

  @Test
  public void topicIsNotNull() {
    conf.set(STREAM.key(), STREAM_NAME);
    assertThat(topic(conf), is(STREAM_NAME));
  }

  @Test(expected = IllegalArgumentException.class)
  public void topicIsNull() {
    conf.set(STREAM.key(), null);
    topic(conf);
  }

}
