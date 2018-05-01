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

import static com.hotels.shunting.yard.common.Utils.checkNotNull;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.APPLICTION_ID;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.BUFFER_CAPACITY;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.MAX_RECORDS;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.REGION;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.STREAM;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.WORKER_ID;
import static com.hotels.shunting.yard.receiver.kinesis.Utils.intProperty;
import static com.hotels.shunting.yard.receiver.kinesis.Utils.stringProperty;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.annotations.VisibleForTesting;

public class DefaultKinesisConsumer implements KinesisConsumer, Closeable {

  private final Configuration conf;
  private KinesisRecordBuffer recordBuffer;
  private final ExecutorService executor;
  private Worker consumer;

  public DefaultKinesisConsumer(Configuration conf) {
    this.conf = conf;
    executor = Executors.newSingleThreadExecutor();
    init();
  }

  private void init() {
    int bufferCapacity = intProperty(conf, BUFFER_CAPACITY);
    recordBuffer = new DefaultKinesisRecordBuffer(bufferCapacity);

    consumer = newWorker(conf);
    executor.execute(consumer);
  }

  @Override
  public Record next() {
    return recordBuffer.get();
  }

  @Override
  public void close() throws IOException {
    consumer.shutdown();
    executor.shutdown();
  }

  private Worker newWorker(Configuration conf) {
    IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(recordBuffer);
    Worker worker = new Worker.Builder()
        .recordProcessorFactory(recordProcessorFactory)
        .config(kinesisProperties(conf))
        .build();
    return worker;
  }

  @VisibleForTesting
  static KinesisClientLibConfiguration kinesisProperties(Configuration conf) {
    String applicationName = checkNotNull(stringProperty(conf, APPLICTION_ID),
        "Property " + APPLICTION_ID + " is not set");
    String streamName = checkNotNull(stringProperty(conf, STREAM), "Property " + STREAM + " is not set");
    String workerId = checkNotNull(stringProperty(conf, WORKER_ID), "Property " + WORKER_ID + " is not set");
    KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(applicationName, streamName,
        new DefaultAWSCredentialsProviderChain(), workerId)
            .withMaxRecords(intProperty(conf, MAX_RECORDS))
            .withRegionName(checkNotNull(stringProperty(conf, REGION), "Property " + REGION + " is not set"));
    return config;
  }

}
