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
package com.hotels.shunting.yard.receiver.kinesis.adapter;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.amazonaws.services.kinesis.model.Record;

class DefaultKinesisRecordBuffer implements KinesisRecordBuffer {

  private final BlockingQueue<Record> messageQueue;

  DefaultKinesisRecordBuffer(int capacity) {
    messageQueue = new ArrayBlockingQueue<>(capacity);
  }

  @Override
  public void put(Record record) {
    try {
      messageQueue.put(record);
    } catch (InterruptedException e) {
      throw new RuntimeException("Unable to write record to blocking queue", e);
    }
  }

  @Override
  public Record get() {
    try {
      return messageQueue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException("Unable to read record from blocking queue", e);
    }
  }

}
