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
package com.hotels.shunting.yard.receiver.kinesis.adapter.consumer;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.shunting.yard.receiver.kinesis.adapter.KinesisConsumer;
import com.hotels.shunting.yard.receiver.kinesis.adapter.KinesisRecordBuffer;
import com.hotels.shunting.yard.receiver.kinesis.adapter.WorkerFactory;

public class DefaultKinesisConsumer implements KinesisConsumer, Closeable {

  private final WorkerFactory workerFactory;
  private final ExecutorService executor;
  private final KinesisRecordBuffer recordBuffer;
  private Worker consumer;

  public DefaultKinesisConsumer(Configuration conf, KinesisRecordBuffer recordBuffer) {
    this(recordBuffer, Executors.newSingleThreadExecutor(),
        WorkerFactory.builder(conf).recordBuffer(recordBuffer).build());
  }

  @VisibleForTesting
  DefaultKinesisConsumer(KinesisRecordBuffer recordBuffer, ExecutorService executor, WorkerFactory workerFactory) {
    this.recordBuffer = recordBuffer;
    this.workerFactory = workerFactory;
    this.executor = executor;
    init();
  }

  private void init() {
    consumer = workerFactory.newWorker();
    executor.execute(consumer);
  }

  @Override
  public Record next() {
    Record record = null;
    do {
      record = recordBuffer.get();
    } while (record == null);
    return record;
  }

  @Override
  public void close() {
    consumer.shutdown();
    executor.shutdown();
  }

}
