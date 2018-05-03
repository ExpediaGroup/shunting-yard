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
package com.hotels.shunting.yard.emitter.kinesis.messaging;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;

import com.hotels.shunting.yard.common.ShuntingYardException;
import com.hotels.shunting.yard.common.messaging.Message;
import com.hotels.shunting.yard.common.messaging.MessageTask;

class KinesisMessageTask implements MessageTask {
  private final KinesisProducer producer;
  private final String stream;
  private final String partition;
  private final byte[] payload;
  private final int retries;

  KinesisMessageTask(KinesisProducer producer, String stream, Message message, int retries) {
    this.producer = producer;
    this.stream = stream;
    partition = message.getQualifiedTableName();
    payload = message.getPayload();
    this.retries = retries;
  }

  @Override
  public void run() {
    Exception lastException = null;
    for (int i = 0; i < retries; ++i) {
      try {
        UserRecordResult result = producer
            .addUserRecord(new UserRecord(stream, partition, ByteBuffer.wrap(payload)))
            .get();
        if (result.isSuccessful()) {
          return;
        }
      } catch (InterruptedException | ExecutionException e) {
        lastException = e;
      }
    }
    String errorMessage = String.format("Unable to deliver message to stream '%s'", stream);
    if (lastException != null) {
      throw new ShuntingYardException(errorMessage, lastException);
    }
    throw new ShuntingYardException(errorMessage);
  }

}
