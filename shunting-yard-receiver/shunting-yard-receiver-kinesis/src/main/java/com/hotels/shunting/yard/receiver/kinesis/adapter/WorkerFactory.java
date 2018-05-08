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

import static com.hotels.shunting.yard.common.Preconditions.checkNotNull;
import static com.hotels.shunting.yard.common.PropertyUtils.intProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.stringProperty;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.APPLICTION_ID;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.MAX_RECORDS;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.REGION;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.STREAM;
import static com.hotels.shunting.yard.receiver.kinesis.KinesisConsumerProperty.WORKER_ID;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.hotels.shunting.yard.receiver.kinesis.adapter.buffer.DefaultKinesisRecordBuffer;

public class WorkerFactory {

  public static class Builder {
    private final Configuration conf;
    private KinesisRecordBuffer recordBuffer;
    private IRecordProcessorFactory recordProcessorFactory;

    private Builder(Configuration conf) {
      Preconditions.checkNotNull(conf, "Configuration is required");
      this.conf = conf;
    }

    public Builder recordBuffer(KinesisRecordBuffer recordBuffer) {
      if (recordBuffer != null && recordProcessorFactory != null) {
        throw new IllegalStateException("Cannot set both recordBuffer and recordProcessorFactory");
      }
      this.recordBuffer = recordBuffer;
      return this;
    }

    public Builder recordProcessorFactory(IRecordProcessorFactory recordProcessorFactory) {
      if (recordProcessorFactory != null && recordBuffer != null) {
        throw new IllegalStateException("Cannot set both recordProcessorFactory and recordBuffer");
      }
      this.recordProcessorFactory = recordProcessorFactory;
      return this;
    }

    public WorkerFactory build() {
      if (recordBuffer == null) {
        recordBuffer = DefaultKinesisRecordBuffer.create(conf);
      }
      if (recordProcessorFactory == null) {
        recordProcessorFactory = new KinesisRecordProcessorFactory(recordBuffer);
      }
      return new WorkerFactory(this);
    }
  }

  public static Builder builder(Configuration conf) {
    return new Builder(conf);
  }

  private final KinesisClientLibConfiguration kinesisProperties;
  private final IRecordProcessorFactory recordProcessorFactory;

  public WorkerFactory(Builder builder) {
    kinesisProperties = kinesisProperties(builder.conf);
    recordProcessorFactory = builder.recordProcessorFactory;
  }

  public Worker newWorker() {
    Worker worker = new Worker.Builder()
        .recordProcessorFactory(recordProcessorFactory)
        .config(kinesisProperties)
        .build();
    return worker;
  }

  @VisibleForTesting
  static KinesisClientLibConfiguration kinesisProperties(Configuration conf) {
    String applicationName = checkNotNull(stringProperty(conf, APPLICTION_ID),
        "Property " + APPLICTION_ID + " is not set");
    String streamName = checkNotNull(stringProperty(conf, STREAM), "Property " + STREAM + " is not set");
    KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(applicationName, streamName,
        new DefaultAWSCredentialsProviderChain(), stringProperty(conf, WORKER_ID))
            .withMaxRecords(intProperty(conf, MAX_RECORDS))
            .withRegionName(checkNotNull(stringProperty(conf, REGION), "Property " + REGION + " is not set"));
    return config;
  }

}
