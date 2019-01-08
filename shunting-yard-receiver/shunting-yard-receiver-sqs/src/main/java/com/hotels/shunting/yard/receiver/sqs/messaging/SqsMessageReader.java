/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.shunting.yard.common.event.ListenerEvent;
import com.hotels.shunting.yard.common.io.SerDeException;
import com.hotels.shunting.yard.common.io.jackson.ApiarySqsMessageDeserializer;
import com.hotels.shunting.yard.common.messaging.MessageReader;

public class SqsMessageReader implements MessageReader {
  private final String queueUrl;
  private final ApiarySqsMessageDeserializer sqsDeserializer;
  private final int waitTimeSeconds;
  private final AmazonSQS consumer;
  private Iterator<Message> records;

  public SqsMessageReader(Configuration conf, ApiarySqsMessageDeserializer sqsDeserializer) {
    this(conf, sqsDeserializer,
        AmazonSQSClientBuilder.standard().withRegion(region(conf)).withCredentials(credentials(conf)).build());
  }

  @VisibleForTesting
  SqsMessageReader(Configuration conf, ApiarySqsMessageDeserializer sqsSerDe, AmazonSQS consumer) {
    queueUrl = queue(conf);
    waitTimeSeconds = waitTimeSeconds(conf);
    this.sqsDeserializer = sqsSerDe;
    this.consumer = consumer;
  }

  @Override
  public void close() throws IOException {
    consumer.shutdown();
  }

  @Override
  public Optional<ListenerEvent> next() {
    readRecordsIfNeeded();
    Message message = records.next();
    delete(message);
    return Optional.of(eventPayLoad(message));
  }

  private void readRecordsIfNeeded() {
    while (records == null || !records.hasNext()) {
      ReceiveMessageRequest request = new ReceiveMessageRequest()
          .withQueueUrl(queueUrl)
          .withWaitTimeSeconds(waitTimeSeconds);
      records = consumer.receiveMessage(request).getMessages().iterator();
    }
  }

  private void delete(Message message) {
    DeleteMessageRequest request = new DeleteMessageRequest()
        .withQueueUrl(queueUrl)
        .withReceiptHandle(message.getReceiptHandle());
    consumer.deleteMessage(request);
  }

  private ListenerEvent eventPayLoad(Message message) {
    try {
      return sqsDeserializer.unmarshal(message.getBody());
    } catch (Exception e) {
      throw new SerDeException("Unable to unmarshall event", e);
    }
  }

}
