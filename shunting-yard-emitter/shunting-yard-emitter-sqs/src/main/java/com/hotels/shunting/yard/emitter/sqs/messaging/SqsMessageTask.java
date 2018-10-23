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
package com.hotels.shunting.yard.emitter.sqs.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import com.hotels.shunting.yard.common.messaging.Message;
import com.hotels.shunting.yard.common.messaging.MessageTask;

class SqsMessageTask implements MessageTask {
  private static final Logger LOG = LoggerFactory.getLogger(SqsMessageTask.class);

  private final AmazonSQS producer;
  private final String topic;
  private final byte[] payload;

  SqsMessageTask(AmazonSQS producer, String topic, Message message) {
    this.producer = producer;
    this.topic = topic;
    payload = message.getPayload();
  }

  @Override
  public void run() {
    LOG.info("Sending message to topic {}", topic);
    producer
        .sendMessage(
            new SendMessageRequest().withQueueUrl(topic).withMessageBody(new String(payload)).withDelaySeconds(0));
  }

}
