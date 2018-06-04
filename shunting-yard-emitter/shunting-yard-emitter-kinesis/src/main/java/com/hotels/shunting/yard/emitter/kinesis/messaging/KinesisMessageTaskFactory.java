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

import static com.hotels.shunting.yard.common.Preconditions.checkNotNull;
import static com.hotels.shunting.yard.common.PropertyUtils.intProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.longProperty;
import static com.hotels.shunting.yard.common.PropertyUtils.stringProperty;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.MAX_CONNECTIONS;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.RECORD_MAX_BUFFERED_TIME;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.REGION;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.REQUEST_TIMEOUT;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.RETRIES;
import static com.hotels.shunting.yard.emitter.kinesis.KinesisProducerProperty.STREAM;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.shunting.yard.common.messaging.Message;
import com.hotels.shunting.yard.common.messaging.MessageTask;
import com.hotels.shunting.yard.common.messaging.MessageTaskFactory;

/**
 * A {@link MessageTaskFactory} that create a task to post message to a Kinesis topic. Note that in order to preserve
 * the order of the events the topic must have only single partition.
 */
public class KinesisMessageTaskFactory implements MessageTaskFactory {

  private final KinesisProducer producer;
  private final String stream;
  private final int retries;

  public KinesisMessageTaskFactory(Configuration conf) {
    this(topic(conf), new KinesisProducer(kinesisProperties(conf)), intProperty(conf, RETRIES));
  }

  @VisibleForTesting
  KinesisMessageTaskFactory(String stream, KinesisProducer producer, int retries) {
    this.producer = producer;
    this.stream = stream;
    this.retries = retries;
  }

  @Override
  public MessageTask newTask(Message message) {
    return new KinesisMessageTask(producer, stream, message, retries);
  }

  @VisibleForTesting
  static KinesisProducerConfiguration kinesisProperties(Configuration conf) {
    KinesisProducerConfiguration config = new KinesisProducerConfiguration();
    config.setRegion(Regions.fromName(stringProperty(conf, REGION)).getName());
    config.setMaxConnections(longProperty(conf, MAX_CONNECTIONS));
    config.setRequestTimeout(longProperty(conf, REQUEST_TIMEOUT));
    config.setRecordMaxBufferedTime(longProperty(conf, RECORD_MAX_BUFFERED_TIME));
    return config;
  }

  @VisibleForTesting
  static String topic(Configuration conf) {
    return checkNotNull(stringProperty(conf, STREAM), "Property " + STREAM + " is not set");
  }

}
