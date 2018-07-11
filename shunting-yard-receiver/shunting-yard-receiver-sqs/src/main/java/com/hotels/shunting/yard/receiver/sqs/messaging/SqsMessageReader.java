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

import static com.hotels.shunting.yard.receiver.sqs.SqsReceiverUtils.credentials;
import static com.hotels.shunting.yard.receiver.sqs.SqsReceiverUtils.queue;
import static com.hotels.shunting.yard.receiver.sqs.SqsReceiverUtils.region;
import static com.hotels.shunting.yard.receiver.sqs.SqsReceiverUtils.waitTimeSeconds;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.SerDeException;
import com.hotels.shunting.yard.common.messaging.MessageReader;

public class SqsMessageReader implements MessageReader {
  private final String queueUrl;
  private final MetaStoreEventSerDe eventSerDe;
  private final int waitTimeSeconds;
  private final AmazonSQS consumer;
  private final MessageDecoder messageDecoder;
  private Iterator<Message> records;

  public SqsMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe) {
    this(conf, eventSerDe,
        AmazonSQSClientBuilder.standard().withRegion(region(conf)).withCredentials(credentials(conf)).build(),
        MessageDecoder.DEFAULT);
  }

  @VisibleForTesting
  SqsMessageReader(
      Configuration conf,
      MetaStoreEventSerDe eventSerDe,
      AmazonSQS consumer,
      MessageDecoder messageDecoder) {
    queueUrl = queue(conf);
    waitTimeSeconds = waitTimeSeconds(conf);
    this.eventSerDe = eventSerDe;
    this.consumer = consumer;
    this.messageDecoder = messageDecoder;
  }

  @Override
  public void close() throws IOException {
    consumer.shutdown();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Cannot remove message from SQS topic");
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public SerializableListenerEvent next() {
    readRecordsIfNeeded();
    Message message = records.next();
    delete(message);
    return eventPayLoad(message);
  }

  private void readRecordsIfNeeded() {
    while (records == null || !records.hasNext()) {
      ReceiveMessageRequest request = new ReceiveMessageRequest().withQueueUrl(queueUrl).withWaitTimeSeconds(
          waitTimeSeconds);
      records = consumer.receiveMessage(request).getMessages().iterator();
    }
  }

  private void delete(Message message) {
    DeleteMessageRequest request = new DeleteMessageRequest().withQueueUrl(queueUrl).withReceiptHandle(
        message.getReceiptHandle());
    consumer.deleteMessage(request);
  }

  private SerializableListenerEvent eventPayLoad(Message message) {
    try {
      return eventSerDe.unmarshal(messageDecoder.decode(message));
    } catch (Exception e) {
      throw new SerDeException("Unable to unmarshall event", e);
    }
  }

}
