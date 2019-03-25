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
import static com.hotels.shunting.yard.receiver.sqs.SqsReceiverUtils.maxMessages;
import static com.hotels.shunting.yard.receiver.sqs.SqsReceiverUtils.queue;
import static com.hotels.shunting.yard.receiver.sqs.SqsReceiverUtils.region;
import static com.hotels.shunting.yard.receiver.sqs.SqsReceiverUtils.waitTimeSeconds;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import com.expedia.apiary.extensions.receiver.common.MessageReader;
import com.expedia.apiary.extensions.receiver.sqs.SqsMessageReader;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageDeserializer;

import com.hotels.shunting.yard.common.messaging.MessageReaderFactory;

public class SqsMessageReaderFactory implements MessageReaderFactory {

  @Override
  public MessageReader newInstance(Configuration conf, SqsMessageDeserializer sqsMessageSerde) {
    AmazonSQS consumer = AmazonSQSClientBuilder.standard()
        .withRegion(region(conf))
        .withCredentials(credentials(conf))
        .build();

    return new SqsMessageReader.Builder(queue(conf), consumer, sqsMessageSerde)
        .withMaxMessages(maxMessages(conf))
        .withWaitTimeSeconds(waitTimeSeconds(conf))
        .build();
  }

}
