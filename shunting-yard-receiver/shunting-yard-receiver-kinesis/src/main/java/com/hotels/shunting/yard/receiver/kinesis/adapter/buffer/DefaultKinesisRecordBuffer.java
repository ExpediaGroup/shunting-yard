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
package com.hotels.shunting.yard.receiver.kinesis.adapter.buffer;

import static com.hotels.shunting.yard.common.PropertyUtils.intProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.longProperty;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.BUFFER_CAPACITY;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.POLLING_TIMEOUT_MS;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.kinesis.model.Record;

import com.hotels.shunting.yard.receiver.kinesis.adapter.KinesisRecordBuffer;

public class DefaultKinesisRecordBuffer implements KinesisRecordBuffer {

  public static DefaultKinesisRecordBuffer create(Configuration conf) {
    return new DefaultKinesisRecordBuffer(intProperty(conf, BUFFER_CAPACITY), longProperty(conf, POLLING_TIMEOUT_MS));
  }

  private final BlockingQueue<Record> messageQueue;
  private final long blockingTimeout;

  private DefaultKinesisRecordBuffer(int capacity, long blockingTimeout) {
    messageQueue = new ArrayBlockingQueue<>(capacity);
    this.blockingTimeout = blockingTimeout;
  }

  @Override
  public boolean put(Record record) {
    try {
      return messageQueue.offer(record, blockingTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Unable to write record to blocking queue", e);
    }
  }

  @Override
  public Record get() {
    try {
      return messageQueue.poll(blockingTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Unable to read record from blocking queue", e);
    }
  }

}
