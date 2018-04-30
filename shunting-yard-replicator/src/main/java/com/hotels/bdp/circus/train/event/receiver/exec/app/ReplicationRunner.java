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
package com.hotels.bdp.circus.train.event.receiver.exec.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circus.train.event.common.event.SerializableAddPartitionEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableAlterPartitionEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableAlterTableEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableCreateTableEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableDropPartitionEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableDropTableEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableInsertEvent;
import com.hotels.bdp.circus.train.event.common.event.SerializableListenerEvent;
import com.hotels.bdp.circus.train.event.common.messaging.MessageReader;
import com.hotels.bdp.circus.train.event.common.receiver.CircusTrainMetaStoreEventListener;

@Component
class ReplicationRunner implements ApplicationRunner, ExitCodeGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationRunner.class);

  private CircusTrainMetaStoreEventListener listener;
  private MessageReader messageReader;

  @Autowired
  ReplicationRunner(MessageReader messageReader, CircusTrainMetaStoreEventListener listener) {
    this.listener = listener;
    this.messageReader = messageReader;
  }

  @Override
  public void run(ApplicationArguments args) {
    while (messageReader.hasNext()) {
      SerializableListenerEvent event = messageReader.next();
      LOG.info("New event received: {}", event);
      // TODO this can be refactored, if we plan to make it more extensible, using small function associated to the
      // class type instead of a listener
      switch (event.getEventType()) {
      case ON_CREATE_TABLE:
        listener.onCreateTable((SerializableCreateTableEvent) event);
        break;
      case ON_ALTER_TABLE:
        listener.onAlterTable((SerializableAlterTableEvent) event);
        break;
      case ON_DROP_TABLE:
        listener.onDropTable((SerializableDropTableEvent) event);
        break;
      case ON_ADD_PARTITION:
        listener.onAddPartition((SerializableAddPartitionEvent) event);
        break;
      case ON_ALTER_PARTITION:
        listener.onAlterPartition((SerializableAlterPartitionEvent) event);
        break;
      case ON_DROP_PARTITION:
        listener.onDropPartition((SerializableDropPartitionEvent) event);
        break;
      case ON_INSERT:
        listener.onInsert((SerializableInsertEvent) event);
        break;
      default:
        LOG.info("Do not know how to process event of type {}", event.getEventType());
        break;
      }
    }
    LOG.info("Finishing event loop");
  }

  @Override
  public int getExitCode() {
    return 0;
  }

}
