/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.expediagroup.shuntingyard.receiver.sqs.messaging;

import static com.expediagroup.shuntingyard.receiver.sqs.SqsReceiverUtils.credentials;
import static com.expediagroup.shuntingyard.receiver.sqs.SqsReceiverUtils.queue;
import static com.expediagroup.shuntingyard.receiver.sqs.SqsReceiverUtils.region;
import static com.expediagroup.shuntingyard.receiver.sqs.SqsReceiverUtils.waitTimeSeconds;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.expediagroup.shuntingyard.common.messaging.MessageReaderFactory;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageReader;

public class SqsMessageReaderFactory implements MessageReaderFactory {

  @Override
  public MessageReader newInstance(Configuration conf) {
    AmazonSQS consumer = AmazonSQSClientBuilder.standard()
        .withRegion(region(conf))
        .withCredentials(credentials(conf))
        .build();

    return new SqsMessageReader.Builder(queue(conf))
        .withConsumer(consumer)
        .withWaitTimeSeconds(waitTimeSeconds(conf))
        .build();
  }

}
