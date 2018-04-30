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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.shunting.yard.common.messaging.Message;
import com.hotels.shunting.yard.emitter.kafka.messaging.KafkaMessageTask;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMessageTaskTest {

  private static final String TOPIC = "topic";
  private static final int PARTITIONS = 2;
  private static final byte[] PAYLOAD = "payload".getBytes();

  private @Mock Producer<Long, byte[]> producer;
  private @Mock Message message;

  private KafkaMessageTask kafkaTask;

  @Before
  public void init() {
    when(message.getQualifiedTableName()).thenReturn("db.table");
    when(message.getPayload()).thenReturn(PAYLOAD);
    kafkaTask = new KafkaMessageTask(producer, TOPIC, PARTITIONS, message);
  }

  @Test
  public void typical() {
    @SuppressWarnings("unchecked")
    ArgumentCaptor<ProducerRecord<Long, byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);

    kafkaTask.run();
    verify(producer).send(captor.capture());
    assertThat(captor.getValue().topic(), is(TOPIC));
    assertThat(captor.getValue().partition(), is(0));
    assertThat(captor.getValue().value(), is(PAYLOAD));
  }

}
