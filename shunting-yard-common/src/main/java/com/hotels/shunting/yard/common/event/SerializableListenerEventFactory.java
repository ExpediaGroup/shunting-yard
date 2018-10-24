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

import static com.hotels.shunting.yard.common.event.CustomEventParameters.HIVE_VERSION;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hive.common.util.HiveVersionInfo;

import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAddPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAlterPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryAlterTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryCreateTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropPartitionEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryDropTableEvent;
import com.hotels.shunting.yard.common.event.apiary.SerializableApiaryInsertTableEvent;

public class SerializableListenerEventFactory {

  private final Configuration config;

  public SerializableListenerEventFactory(Configuration config) {
    this.config = config;
  }

  private <T extends ListenerEvent> T addParams(T event) {
    event.putParameter(HIVE_VERSION.varname(), HiveVersionInfo.getVersion());
    event.putParameter(METASTOREURIS.varname, config.get(METASTOREURIS.varname));
    return event;
  }

  public SerializableApiaryCreateTableEvent create(CreateTableEvent event) {
    return new SerializableApiaryCreateTableEvent(addParams(event));
  }

  public SerializableApiaryAlterTableEvent create(AlterTableEvent event) {
    return new SerializableApiaryAlterTableEvent(addParams(event));
  }

  public SerializableApiaryDropTableEvent create(DropTableEvent event) {
    return new SerializableApiaryDropTableEvent(addParams(event));
  }

  public SerializableApiaryAddPartitionEvent create(AddPartitionEvent event) {
    return new SerializableApiaryAddPartitionEvent(addParams(event));
  }

  public SerializableApiaryAlterPartitionEvent create(AlterPartitionEvent event) {
    return new SerializableApiaryAlterPartitionEvent(addParams(event));
  }

  public SerializableApiaryDropPartitionEvent create(DropPartitionEvent event) {
    return new SerializableApiaryDropPartitionEvent(addParams(event));
  }

  public SerializableApiaryInsertTableEvent create(InsertEvent event) {
    return new SerializableApiaryInsertTableEvent(addParams(event));
  }

}
