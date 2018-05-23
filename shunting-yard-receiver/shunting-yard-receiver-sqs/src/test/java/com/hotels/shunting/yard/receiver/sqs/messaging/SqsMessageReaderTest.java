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
package com.hotels.shunting.yard.receiver.sqs.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.shunting.yard.receiver.sqs.SqsConsumerProperty.QUEUE;
import static com.hotels.shunting.yard.receiver.sqs.SqsConsumerProperty.WAIT_TIME_SECONDS;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.common.collect.ImmutableList;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.SerDeException;

@RunWith(MockitoJUnitRunner.class)
public class SqsMessageReaderTest {

  private static final String QUEUE_NAME = "queue";
  private static final int WAIT_TIME = 1;
  private static final String RECEIPT_HANDLER = "receipt_handler";
  private static final byte[] MESSAGE_CONTENT = "message".getBytes();

  private @Mock MetaStoreEventSerDe serDe;
  private @Mock AmazonSQS consumer;
  private @Mock MessageDecoder decoder;
  private @Mock ReceiveMessageResult receiveMessageResult;
  private @Mock List<Message> messages;
  private @Mock Iterator<Message> messageIterator;
  private @Mock Message message;
  private @Mock SerializableListenerEvent event;

  private @Captor ArgumentCaptor<ReceiveMessageRequest> receiveMessageRequestCaptor;
  private @Captor ArgumentCaptor<DeleteMessageRequest> deleteMessageRequestCaptor;

  private final Configuration conf = new Configuration();
  private SqsMessageReader reader;

  @Before
  public void init() throws Exception {
    when(consumer.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);
    when(receiveMessageResult.getMessages()).thenReturn(messages);
    when(messages.iterator()).thenReturn(messageIterator);
    when(messageIterator.hasNext()).thenReturn(true, false);
    when(messageIterator.next()).thenReturn(message).thenThrow(NoSuchElementException.class);
    when(message.getReceiptHandle()).thenReturn(RECEIPT_HANDLER);
    when(decoder.decode(message)).thenReturn(MESSAGE_CONTENT);
    when(serDe.unmarshal(MESSAGE_CONTENT)).thenReturn(event);
    conf.set(QUEUE.key(), QUEUE_NAME);
    conf.set(WAIT_TIME_SECONDS.key(), String.valueOf(WAIT_TIME));
    reader = new SqsMessageReader(conf, serDe, consumer, decoder);
  }

  @Test
  public void close() throws Exception {
    reader.close();
    verify(consumer).shutdown();
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
    verify(consumer).receiveMessage(receiveMessageRequestCaptor.capture());
    assertThat(receiveMessageRequestCaptor.getValue().getQueueUrl()).isEqualTo(QUEUE_NAME);
    assertThat(receiveMessageRequestCaptor.getValue().getWaitTimeSeconds()).isEqualTo(WAIT_TIME);
    verify(consumer).deleteMessage(deleteMessageRequestCaptor.capture());
    assertThat(deleteMessageRequestCaptor.getValue().getQueueUrl()).isEqualTo(QUEUE_NAME);
    assertThat(deleteMessageRequestCaptor.getValue().getReceiptHandle()).isEqualTo(RECEIPT_HANDLER);
    verify(decoder).decode(message);
    verify(serDe).unmarshal(MESSAGE_CONTENT);
  }

  @Test
  public void nextReadsNoRecordsFromQueue() throws Exception {
    when(receiveMessageResult.getMessages()).thenReturn(ImmutableList.<Message> of()).thenReturn(messages);
    reader.next();
    verify(consumer, times(2)).receiveMessage(any(ReceiveMessageRequest.class));
    verify(consumer).deleteMessage(any(DeleteMessageRequest.class));
    verify(decoder).decode(message);
    verify(serDe).unmarshal(MESSAGE_CONTENT);
  }

  @Test(expected = SerDeException.class)
  public void decoderThrowsException() {
    when(decoder.decode(any(Message.class))).thenThrow(RuntimeException.class);
    reader.next();
  }

  @Test(expected = SerDeException.class)
  public void unmarhsallThrowsException() throws Exception {
    when(serDe.unmarshal(any(byte[].class))).thenThrow(RuntimeException.class);
    reader.next();
  }

}
