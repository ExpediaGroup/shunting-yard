/**
 * Copyright (C) 2016-2019 Expedia Inc.
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

import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

/**
 * To make processing event in the receiver easier.
 */
public enum EventType {
  ON_CREATE_TABLE(CreateTableEvent.class),
  ON_ALTER_TABLE(AlterTableEvent.class),
  ON_DROP_TABLE(DropTableEvent.class),
  ON_ADD_PARTITION(AddPartitionEvent.class),
  ON_ALTER_PARTITION(AlterPartitionEvent.class),
  ON_DROP_PARTITION(DropPartitionEvent.class),
  ON_INSERT(InsertEvent.class);

  private final Class<? extends ListenerEvent> eventClass;

  private EventType(Class<? extends ListenerEvent> eventClass) {
    if (eventClass == null) {
      throw new NullPointerException("Parameter eventClass is required");
    }
    this.eventClass = eventClass;
  }

  public Class<? extends ListenerEvent> eventClass() {
    return eventClass;
  }

  public static EventType forClass(Class<? extends ListenerEvent> clazz) {
    for (EventType e : values()) {
      if (e.eventClass().equals(clazz)) {
        return e;
      }
    }
    throw new IllegalArgumentException("EventType not found for class " + clazz.getName());
  }

}
