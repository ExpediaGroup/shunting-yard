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
package com.hotels.bdp.circus.train.event.emitter.kinesis.listener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circus.train.event.common.emitter.AbstractMetaStoreEventListener;
import com.hotels.bdp.circus.train.event.common.event.SerializableListenerEventFactory;
import com.hotels.bdp.circus.train.event.common.io.MetaStoreEventSerDe;
import com.hotels.bdp.circus.train.event.common.messaging.MessageTaskFactory;
import com.hotels.bdp.circus.train.event.emitter.kinesis.messaging.KinesisMessageTaskFactory;

public class KinesisMetaStoreEventListener extends AbstractMetaStoreEventListener {

  private MetaStoreEventSerDe eventSerDe;
  private MessageTaskFactory messageTaskFactory;

  public KinesisMetaStoreEventListener(Configuration config) {
    this(config, new SerializableListenerEventFactory(config), new MetaStoreEventSerDe(),
        new KinesisMessageTaskFactory(config), Executors.newSingleThreadExecutor());
  }

  @VisibleForTesting
  KinesisMetaStoreEventListener(
      Configuration config,
      SerializableListenerEventFactory serializableListenerEventFactory,
      MetaStoreEventSerDe eventSerDe,
      MessageTaskFactory messageTaskFactory,
      ExecutorService executorService) {
    super(config, serializableListenerEventFactory, executorService);
    this.eventSerDe = eventSerDe;
    this.messageTaskFactory = messageTaskFactory;
  }

  @Override
  protected MetaStoreEventSerDe getMetaStoreEventSerDe() {
    return eventSerDe;
  }

  @Override
  protected MessageTaskFactory getMessageTaskFactory() {
    return messageTaskFactory;
  }

}
