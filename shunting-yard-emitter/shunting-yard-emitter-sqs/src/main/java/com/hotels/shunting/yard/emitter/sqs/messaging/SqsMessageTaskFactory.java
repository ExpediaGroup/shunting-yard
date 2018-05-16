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

import static com.hotels.shunting.yard.emitter.sqs.SqsEmitterUtils.credentials;
import static com.hotels.shunting.yard.emitter.sqs.SqsEmitterUtils.groupId;
import static com.hotels.shunting.yard.emitter.sqs.SqsEmitterUtils.queue;
import static com.hotels.shunting.yard.emitter.sqs.SqsEmitterUtils.region;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.shunting.yard.common.messaging.Message;
import com.hotels.shunting.yard.common.messaging.MessageTask;
import com.hotels.shunting.yard.common.messaging.MessageTaskFactory;

/**
 * A {@link MessageTaskFactory} that create a task to post message to a SNS topic.
 */
public class SqsMessageTaskFactory implements MessageTaskFactory {

  private final AmazonSQS producer;
  private final String topic;
  private final String messageGroupId;

  public SqsMessageTaskFactory(Configuration conf) {
    this(queue(conf), groupId(conf),
        AmazonSQSClientBuilder.standard().withRegion(region(conf)).withCredentials(credentials(conf)).build());
  }

  @VisibleForTesting
  SqsMessageTaskFactory(String topic, String messageGroupId, AmazonSQS producer) {
    this.producer = producer;
    this.topic = topic;
    this.messageGroupId = messageGroupId;
  }

  @Override
  public MessageTask newTask(Message message) {
    return new SqsMessageTask(producer, topic, messageGroupId, message);
  }

}
