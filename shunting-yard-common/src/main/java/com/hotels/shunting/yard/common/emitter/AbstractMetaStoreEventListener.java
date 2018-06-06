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
package com.hotels.shunting.yard.common.emitter;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.shunting.yard.common.event.SerializableListenerEvent;
import com.hotels.shunting.yard.common.event.SerializableListenerEventFactory;
import com.hotels.shunting.yard.common.io.MetaStoreEventSerDe;
import com.hotels.shunting.yard.common.messaging.Message;
import com.hotels.shunting.yard.common.messaging.MessageTask;
import com.hotels.shunting.yard.common.messaging.MessageTaskFactory;
import com.hotels.shunting.yard.common.metrics.MetricsConstant;
import com.hotels.shunting.yard.common.metrics.MetricsHelper;

public abstract class AbstractMetaStoreEventListener extends MetaStoreEventListener {
  private static final Logger log = LoggerFactory.getLogger(AbstractMetaStoreEventListener.class);

  static void success() {
    MetricsHelper.incrementCounter(MetricsConstant.EMITTER_SUCCESSES);
  }

  static void error(Exception e) {
    // ERROR, ShuntingYard and Emitter are keywords
    log.error("Error in ShuntingYard Emitter", e);
    MetricsHelper.incrementCounter(MetricsConstant.EMITTER_FAILURES);
  }

  private final SerializableListenerEventFactory serializableListenerEventFactory;
  private final ExecutorService executorService;

  protected AbstractMetaStoreEventListener(
      Configuration config,
      SerializableListenerEventFactory serializableListenerEventFactory,
      ExecutorService executorService) {
    super(config);
    log.info("Creating MetaStoreEventListener of class {}", this.getClass().getName());
    this.serializableListenerEventFactory = serializableListenerEventFactory;
    this.executorService = executorService;
  }

  protected abstract MetaStoreEventSerDe getMetaStoreEventSerDe();

  protected abstract MessageTaskFactory getMessageTaskFactory();

  private MessageTask message(Message message) {
    return new WrappingMessageTask(getMessageTaskFactory().newTask(message));
  }

  private Message withPayload(SerializableListenerEvent event) throws MetaException {
    return Message
        .builder()
        .database(event.getDatabaseName())
        .table(event.getTableName())
        .payload(getMetaStoreEventSerDe().marshal(event))
        .build();
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    log.info("Create table event received");
    try {
      executorService.submit(message(withPayload(serializableListenerEventFactory.create(tableEvent))));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    log.info("Drop table event received");
    try {
      executorService.submit(message(withPayload(serializableListenerEventFactory.create(tableEvent))));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    log.info("Alter table event received");
    try {
      executorService.submit(message(withPayload(serializableListenerEventFactory.create(tableEvent))));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    log.info("Add partition event received");
    try {
      executorService.submit(message(withPayload(serializableListenerEventFactory.create(partitionEvent))));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    log.info("Drop partition event received");
    try {
      executorService.submit(message(withPayload(serializableListenerEventFactory.create(partitionEvent))));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
    log.info("Alter partition event received");
    try {
      executorService.submit(message(withPayload(serializableListenerEventFactory.create(partitionEvent))));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onInsert(InsertEvent insertEvent) throws MetaException {
    log.info("Insert event received");
    try {
      executorService.submit(message(withPayload(serializableListenerEventFactory.create(insertEvent))));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onConfigChange(ConfigChangeEvent tableEvent) throws MetaException {}

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {}

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {}

  @Override
  public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {}

  @Override
  public void onAddIndex(AddIndexEvent indexEvent) throws MetaException {}

  @Override
  public void onDropIndex(DropIndexEvent indexEvent) throws MetaException {}

  @Override
  public void onAlterIndex(AlterIndexEvent indexEvent) throws MetaException {}

  @Override
  public void onCreateFunction(CreateFunctionEvent fnEvent) throws MetaException {}

  @Override
  public void onDropFunction(DropFunctionEvent fnEvent) throws MetaException {}

}
