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
package com.hotels.bdp.circus.train.event.receiver.kinesis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circus.train.event.common.event.SerializableListenerEvent;
import com.hotels.bdp.circus.train.event.common.io.MetaStoreEventSerDe;
import com.hotels.bdp.circus.train.event.common.messaging.MessageReader;
import com.hotels.bdp.circus.train.event.receiver.kinesis.consumer.DefaultKinesisConsumer;
import com.hotels.bdp.circus.train.event.receiver.kinesis.consumer.KinesisConsumer;

public class KinesisMessageReader implements MessageReader {

  private final Configuration conf;
  private final KinesisConsumer consumer;
  private final MetaStoreEventSerDe eventSerDe;

  public KinesisMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe) {
    this(conf, eventSerDe, new DefaultKinesisConsumer(conf));
  }

  @VisibleForTesting
  KinesisMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe, KinesisConsumer consumer) {
    this.conf = conf;
    this.consumer = consumer;
    this.eventSerDe = eventSerDe;
  }

  @Override
  public void close() throws IOException {
    consumer.close();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Cannot remove message from Kafka topic");
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public SerializableListenerEvent next() {
    return eventPayLoad(consumer.next().getData().array());
  }

  private SerializableListenerEvent eventPayLoad(byte[] data) {
    try {
      return eventSerDe.unmarshall(data);
    } catch (MetaException e) {
      throw new RuntimeException("Unable to unmarshall event", e);
    }
  }

}
