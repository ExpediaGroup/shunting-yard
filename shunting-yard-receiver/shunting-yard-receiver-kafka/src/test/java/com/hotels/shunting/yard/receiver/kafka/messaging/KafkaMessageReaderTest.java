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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.shunting.yard.receiver.kafka.KafkaConsumerProperty.TOPIC;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.SerDeException;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMessageReaderTest {

  private static final String TOPIC_NAME = "topic";
  private static final int PARTITION = 0;
  private static final byte[] MESSAGE_CONTENT = "message".getBytes();

  private @Mock MetaStoreEventSerDe serDe;
  private @Mock KafkaConsumer<Long, byte[]> consumer;
  private @Mock ConsumerRecord<Long, byte[]> message;
  private @Mock SerializableListenerEvent event;

  private final Configuration conf = new Configuration();
  private ConsumerRecords<Long, byte[]> messages;
  private KafkaMessageReader reader;

  @Before
  public void init() throws Exception {
    List<ConsumerRecord<Long, byte[]>> messageList = ImmutableList.of(message);
    Map<TopicPartition, List<ConsumerRecord<Long, byte[]>>> messageMap = ImmutableMap
        .of(new TopicPartition(TOPIC_NAME, PARTITION), messageList);
    messages = new ConsumerRecords<>(messageMap);
    when(consumer.poll(anyLong())).thenReturn(messages);
    when(message.value()).thenReturn(MESSAGE_CONTENT);
    when(serDe.unmarshal(MESSAGE_CONTENT)).thenReturn(event);
    conf.set(TOPIC.key(), TOPIC_NAME);
    reader = new KafkaMessageReader(conf, serDe, consumer);
  }

  @Test
  public void close() throws Exception {
    reader.close();
    verify(consumer).close();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void remove() {
    reader.remove();
  }

  @Test
  public void hasNext() {
    assertThat(reader.hasNext()).isTrue();
  }

  @Test
  public void nextReadsRecordsFromQueue() throws Exception {
    assertThat(reader.next()).isSameAs(event);
    verify(consumer).poll(anyLong());
    verify(serDe).unmarshal(MESSAGE_CONTENT);
  }

  @Test
  public void nextReadsNoRecordsFromQueue() throws Exception {
    when(consumer.poll(anyLong())).thenReturn(ConsumerRecords.<Long, byte[]> empty()).thenReturn(messages);
    reader.next();
    verify(consumer, times(2)).poll(anyLong());
    verify(serDe).unmarshal(MESSAGE_CONTENT);
  }

  @Test(expected = SerDeException.class)
  public void unmarhsallThrowsException() throws Exception {
    when(serDe.unmarshal(any(byte[].class))).thenThrow(RuntimeException.class);
    reader.next();
  }

}
