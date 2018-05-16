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
package com.hotels.shunting.yard.common.event;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

public class SerializableListenerEventFactory {

  private final Configuration config;

  public SerializableListenerEventFactory(Configuration config) {
    this.config = config;
  }

  private <T extends ListenerEvent> T addParams(T event) {
    event.putParameter(METASTOREURIS.varname, config.get(METASTOREURIS.varname));
    return event;
  }

  public SerializableCreateTableEvent create(CreateTableEvent event) {
    return new SerializableCreateTableEvent(addParams(event));
  }

  public SerializableAlterTableEvent create(AlterTableEvent event) {
    return new SerializableAlterTableEvent(addParams(event));
  }

  public SerializableDropTableEvent create(DropTableEvent event) {
    return new SerializableDropTableEvent(addParams(event));
  }

  public SerializableAddPartitionEvent create(AddPartitionEvent event) {
    return new SerializableAddPartitionEvent(addParams(event));
  }

  public SerializableAlterPartitionEvent create(AlterPartitionEvent event) {
    return new SerializableAlterPartitionEvent(addParams(event));
  }

  public SerializableDropPartitionEvent create(DropPartitionEvent event) {
    return new SerializableDropPartitionEvent(addParams(event));
  }

  public SerializableInsertEvent create(InsertEvent event) {
    return new SerializableInsertEvent(addParams(event));
  }

}
