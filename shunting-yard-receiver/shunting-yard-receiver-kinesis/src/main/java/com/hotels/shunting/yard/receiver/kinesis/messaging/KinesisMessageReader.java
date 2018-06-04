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
package com.hotels.shunting.yard.receiver.kinesis.messaging;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.io.SerDeException;
import com.hotels.shunting.yard.common.messaging.MessageReader;
import com.hotels.shunting.yard.receiver.kinesis.adapter.KinesisConsumer;
import com.hotels.shunting.yard.receiver.kinesis.adapter.buffer.DefaultKinesisRecordBuffer;
import com.hotels.shunting.yard.receiver.kinesis.adapter.consumer.DefaultKinesisConsumer;

public class KinesisMessageReader implements MessageReader {

  private final KinesisConsumer consumer;
  private final MetaStoreEventSerDe eventSerDe;

  public KinesisMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe) {
    this(eventSerDe, new DefaultKinesisConsumer(conf, DefaultKinesisRecordBuffer.create(conf)));
  }

  @VisibleForTesting
  KinesisMessageReader(MetaStoreEventSerDe eventSerDe, KinesisConsumer consumer) {
    this.consumer = consumer;
    this.eventSerDe = eventSerDe;
  }

  @Override
  public void close() throws IOException {
    consumer.close();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Cannot remove message from Kinesis stream");
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
      return eventSerDe.unmarshal(data);
    } catch (Exception e) {
      // TODO this may be removed when we get rid of checked exceptions in the SerDe contract
      throw new SerDeException("Unable to unmarshall event", e);
    }
  }

}
