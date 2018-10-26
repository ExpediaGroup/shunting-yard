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

/**
 * To make processing event in the receiver easier.
 */
public enum EventType {
  ADD_PARTITION(AddPartitionEvent.class),
  ALTER_PARTITION(AlterPartitionEvent.class),
  DROP_PARTITION(DropPartitionEvent.class),
  CREATE_TABLE(CreateTableEvent.class),
  INSERT(InsertTableEvent.class),
  DROP_TABLE(DropTableEvent.class),
  ALTER_TABLE(AlterTableEvent.class);

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
